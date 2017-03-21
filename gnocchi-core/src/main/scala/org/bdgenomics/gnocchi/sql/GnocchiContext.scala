/**
 * Licensed to Big Data Genomics (BDG) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The BDG licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bdgenomics.gnocchi.sql

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ Column, DataFrame, Dataset, SQLContext }
import org.apache.spark.sql.functions._
import org.bdgenomics.gnocchi.models.{Association, GnocchiModel, Phenotype, GenotypeState}

object GnocchiContext {

  implicit def gcFromSqlAndSparkContext(sc: SparkContext, sqlContext: SQLContext): GnocchiSqlContext =
    new GnocchiSqlContext(sc, sqlContext)
}

class GnocchiSqlContext private[sql] (@transient sc: SparkContext, @transient sqlContext: SQLContext) extends Serializable {

  import sqlContext.implicits._

  def toGenotypeStateDataset(gtFrame: DataFrame, ploidy: Int): Dataset[GenotypeState] = {
    toGenotypeStateDataFrame(gtFrame, ploidy).as[GenotypeState]
  }

  def toGenotypeStateDataFrame(gtFrame: DataFrame, ploidy: Int, sparse: Boolean = false): DataFrame = {

    val filteredGtFrame = if (sparse) {
      // if we want the sparse representation, we prefilter
      val sparseFilter = (0 until ploidy).map(i => {
        gtFrame("alleles").getItem(i) !== "Ref"
      }).reduce(_ || _)
      gtFrame.filter(sparseFilter)
    } else {
      gtFrame
    }

    // generate expression
    val genotypeState = (0 until ploidy).map(i => {
      val c: Column = when(filteredGtFrame("alleles").getItem(i) === "Ref", 1).otherwise(0)
      c
    }).reduce(_ + _)

    val missingGenotypes = (0 until ploidy).map(i => {
      val c: Column = when(filteredGtFrame("alleles").getItem(i) === "NoCall", 1).otherwise(0)
      c
    }).reduce(_ + _)

    filteredGtFrame.select(filteredGtFrame("variant.contig.contigName").as("contig"),
      filteredGtFrame("variant.start").as("start"),
      filteredGtFrame("variant.end").as("end"),
      filteredGtFrame("variant.referenceAllele").as("ref"),
      filteredGtFrame("variant.alternateAllele").as("alt"),
      filteredGtFrame("sampleId"),
      genotypeState.as("genotypeState"),
      missingGenotypes.as("missingGenotypes"))
  }

  def loadPhenotypes(oneTwo: Boolean, file: String, phenoName: String, covarFile: String = "", covarNames: String = ""): Unit = {
    require((covarFile.length == 0 && covarNames.length == 0) || (covarFile.length != 0 && covarNames.length != 0),
    "Must have Covariate names with Covariate files")

    println("Loading phenotypes from %s.".format(file))

    // get the relevant parts of the phenotypes file and put into a DF
    val phenotypes = sc.textFile(file).persist()
    val covars = sc.textFile(covarFile).persist()
    println("Loading covars from %s.".format(covarFile))

    // separate header and data
    val header = phenotypes.first()
    val covarHeader = covars.first()

    // get label indices
    val covarLabels = if (covarHeader.split("\t").length >= 2) {
        covarHeader.split("\t").zipWithIndex
      } else {
        covarHeader.split(" ").zipWithIndex
      }

    require(covarLabels.length >= 2, "Covars file must have a minimum of 2 tab delimited columns. The first being some form of sampleID, the rest being covar values. A header with column labels must also be present. ")
    val labels = if (header.split("\t").length >= 2) {
        header.split("\t").zipWithIndex
      } else {
        header.split(" ").zipWithIndex
      }

    require(labels.length >= 2, "Phenotypes file must have a minimum of 2 tab delimited columns. The first being some form of sampleID, the rest being phenotype values. A header with column labels must also be present. ")

    // extract covarNames
    val covariates = covarNames.split(",")

    // get the index of the phenotype to be regressed
    val primaryPhenoIndex = labels.find(_._1 == phenoName)
    require(primaryPhenoIndex.nonEmpty, "The phenoName given doesn't match any of the phenotypes specified in the header.")

    // get the indices of the covariates
    // TODO Clean this up, can remove a lot of excess computation from these loops
    val covarIndices = new Array[Int](covariates.length)
    var i = 0
    for (covar <- covariates) {
      var hasMatch = false
      require(covar == phenoName, "One or more of the covariates has the same name as phenoName.")
      for (labelpair <- covarLabels) {
        if (labelpair._1 == covar) {
          hasMatch = true
          covarIndices(i) = labelpair._2
          i = i + 1
        }
      }
      require(hasMatch, "One or more of the names from covarNames doesn't match a column title in the header of the phenotype file.")
    }

    // construct the phenotypes RDD, filtering out all samples that don't have the phenotype or one of the covariates
    getAndFilterPhenotypes(oneTwo, phenotypes, header, covars, covarHeader, primaryPhenoIndex.get._2, covarIndices)
  }

  private def getAndFilterPhenotypes(
    oneTwo: Boolean,
    phenotypes: RDD[String],
    header: String,
    covars: RDD[String],
    covarHeader: String,
    primaryPhenoIndex: Int,
    covarIndices: Array[Int]): RDD[Phenotype] = {

    // TODO: NEED TO REQUIRE THAT ALL THE PHENOTPES BE REPRESENTED BY NUMBERS.
    // split up the header for making the phenotype label later

    val headerTabDelimited = header.split("\t").length != 1
    val splitHeader =
      if (headerTabDelimited) {
        header.split("\t")
      } else {
        header.split(" ")
      }

    val covarTabDelimited = covarHeader.split("\t").length != 1
    val splitCovarHeader = if (covarTabDelimited) {
        covarHeader.split("\t")
      } else {
        covarHeader.split(" ")
      }

    val fullHeader = splitHeader ++ splitCovarHeader
    val mergedIndices = covarIndices.map(elem => { elem + splitHeader.length })

    // construct the RDD of Phenotype objects from the data in the textfile
    val indices = Array(primaryPhenoIndex) ++ mergedIndices
    val covarData = covars.filter(line => line != covarHeader)
      .map(line => {
        if (covarTabDelimited) {
          line.split("\t")
        } else {
          line.split(" ")
        }
      }).keyBy(splitLine => splitLine(0)).filter(_._1 != "")

    val data = phenotypes.filter(line => line != header)
      // split the line by column
      .map(line => {
      if(headerTabDelimited) {
        line.split("\t")
      } else {
        line.split(" ")
      }
    }).keyBy(splitLine => splitLine(0)).filter(_._1 != "")

    val joinedData = data.cogroup(covarData).map(pair => {
      val (sampleId, (phenosIterable, covariatesIterable)) = pair
      val phenoArray = phenosIterable.toList.head
      val covarArray = covariatesIterable.toList.head
      val toret = phenoArray ++ covarArray
      toret
    })

    // filter out empty lines and samples missing the phenotype being regressed. Missing values denoted by -9.0
    // TODO CLEAN THIS UP
    val finalData = joinedData.filter(p => {
      if (p.length > 2) {

        var keep = true
        for (valueIndex <- indices) {
          if (isMissing(p(valueIndex))) {
            keep = false
          }
        }
        keep
      } else {
        false
      }
    }).map(p => {
      if (oneTwo) {
        val toRet = p.slice(0, primaryPhenoIndex) ++ List((p(primaryPhenoIndex).toDouble - 1).toString) ++ p.slice(primaryPhenoIndex + 1, p.length)
        toRet
      } else {
        p
      }
    })
      // construct a phenotype object from the data in the sample
      // TODO Get rid of for..yield here
      .map(p => new Phenotype(
      (for (i <- indices) yield fullHeader(i)).mkString(","), // phenotype labels string
      p(0), // sampleID string
      for (i <- indices) yield p(i).toDouble) // phenotype values
      .asInstanceOf[Phenotype])
    // unpersist the textfile
    phenotypes.unpersist()
    covars.unpersist()

    finalData
  }



  private def isMissing(value: String): Boolean = {
    try {
      value.toDouble == -9.0
    } catch {
      case e: java.lang.NumberFormatException => true
    }
  }

  def buildModel(rdd: RDD[GenotypeState],
                 phenotypes: RDD[Phenotype],
                 save: Boolean = false): (GnocchiModel, RDD[Association]) = {

    // call RegressPhenotypes on the data
    val assocs = fit(rdd, phenotypes)

    // extract the model parameters (including p-value) for each variant and build LogisticGnocchiModel
    val model = extractModel(assocs, sc)

    (model, assocs)
  }

  
}
