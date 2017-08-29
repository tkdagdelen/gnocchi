package net.fnothaft.gnocchi.sql

import java.io.Serializable

import net.fnothaft.gnocchi.models.GnocchiModelMetaData
import net.fnothaft.gnocchi.models.linear.{ AdditiveLinearGnocchiModel, DominantLinearGnocchiModel }
import net.fnothaft.gnocchi.models.logistic.{ AdditiveLogisticGnocchiModel, DominantLogisticGnocchiModel }
import net.fnothaft.gnocchi.models.variant.QualityControlVariantModel
import net.fnothaft.gnocchi.models.variant.linear.{ AdditiveLinearVariantModel, DominantLinearVariantModel }
import net.fnothaft.gnocchi.models.variant.logistic.{ AdditiveLogisticVariantModel, DominantLogisticVariantModel }
import net.fnothaft.gnocchi.primitives.genotype.{ Genotype, GenotypeState }
import net.fnothaft.gnocchi.primitives.phenotype.{ BetterPhenotype, Phenotype }
import net.fnothaft.gnocchi.primitives.variants.{ CalledVariant }
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ Column, DataFrame, Dataset, SparkSession }
import org.apache.spark.sql.functions.{ concat, lit, when, array, typedLit, udf, col }
import org.apache.spark.sql.types.{ ArrayType, DoubleType }
import org.bdgenomics.adam.cli.Vcf2ADAM
import org.bdgenomics.formats.avro.{ Contig, Variant }
import org.bdgenomics.utils.misc.Logging

import scala.io.Source.fromFile

object GnocchiSession {

  // Add GnocchiContext methods
  implicit def sparkContextToGnocchiContext(sc: SparkContext): GnocchiSession =
    new GnocchiSession(sc)

}

class GnocchiSession(@transient val sc: SparkContext) extends Serializable with Logging {

  val sparkSession = SparkSession.builder().getOrCreate()
  import sparkSession.implicits._

  //  def convertVCF(vcfPath: String,
  //                 destination: String,
  //                 overwrite: Boolean): Path = {
  //
  //    val adamDestination = new Path(destination)
  //    val parquetFiles = new Path(vcfPath)
  //    val fs = adamDestination.getFileSystem(sc.hadoopConfiguration)
  //
  //    if (!fs.exists(parquetFiles)) {
  //      val cmdLine: Array[String] = Array[String](vcfPath, destination)
  //      Vcf2ADAM(cmdLine).run(sc)
  //    } else if (overwrite) {
  //      fs.delete(parquetFiles, true)
  //      val cmdLine: Array[String] = Array[String](vcfPath, destination)
  //      Vcf2ADAM(cmdLine).run(sc)
  //    }
  //
  //    adamDestination
  //  }

  //  def loadGenotypes(genotypesPath: String,
  //                    ploidy: Int,
  //                    mind: Option[Double],
  //                    maf: Option[Double],
  //                    geno: Option[Double]): DataFrame = {
  //
  //    import sparkSession.implicits._
  //
  //    val genotypes = sparkSession.read.format("parquet").load(genotypesPath)
  //
  //    val genotypeDF = toGenotypeStateDataFrame(genotypes, ploidy)
  //
  //    val genoStatesWithNames = genotypeDF.select(
  //      concat($"contigName", lit("_"), $"end", lit("_"), $"alt") as "contigName",
  //      genotypeDF("start"),
  //      genotypeDF("end"),
  //      genotypeDF("ref"),
  //      genotypeDF("alt"),
  //      genotypeDF("sampleId"),
  //      genotypeDF("genotypeState"),
  //      genotypeDF("missingGenotypes"),
  //      genotypeDF("phaseSetId"))
  //
  ////    val sampleFilteredDF = filterSamples(genoStatesWithNames, mind)
  ////
  ////    val genoFilteredDF = filterVariants(sampleFilteredDF, geno, maf)
  //
  ////    genoFilteredDF
  //
  //    //    val finalGenotypeStatesRdd = genoFilteredDF.filter($"missingGenotypes" != 2)
  //
  //    //    finalGenotypeStatesRdd
  //  }

  private def toGenotypeStateDataFrame(gtFrame: DataFrame, ploidy: Int): DataFrame = {
    // generate expression
    val genotypeState = (0 until ploidy).map(i => {
      val c: Column = when(gtFrame("alleles").getItem(i) === "REF", 1).otherwise(0)
      c
    }).reduce(_ + _)

    val missingGenotypes = (0 until ploidy).map(i => {
      val c: Column = when(gtFrame("alleles").getItem(i) === "NO_CALL", 1).otherwise(0)
      c
    }).reduce(_ + _)

    // is this correct? or should we change the column to nullable?
    val phaseSetId: Column = when(gtFrame("phaseSetId").isNull, 0).otherwise(gtFrame("phaseSetId"))

    gtFrame.select(gtFrame("variant.contigName").as("contigName"),
      gtFrame("variant.start").as("start"),
      gtFrame("variant.end").as("end"),
      gtFrame("variant.referenceAllele").as("ref"),
      gtFrame("variant.alternateAllele").as("alt"),
      gtFrame("sampleId"),
      genotypeState.as("genotypeState"),
      missingGenotypes.as("missingGenotypes"),
      phaseSetId.as("phaseSetId"))
  }

  //  def filterSamples(genotype: RDD[Genotype], mind: Option[Double]): RDD[Genotype] = {
  //    val mindF = sc.broadcast(mind)
  //
  //    val samples = genotype.map(gs => (gs.sampleId, gs.missingGenotypes, 2))
  //      .keyBy(_._1)
  //      .reduceByKey((tup1, tup2) => (tup1._1, tup1._2 + tup2._2, tup1._3 + tup2._3))
  //      .map(keyed_tup => {
  //        val (key, tuple) = keyed_tup
  //        val (sampleId, missCount, total) = tuple
  //        val mind = missCount.toDouble / total.toDouble
  //        (sampleId, mind)
  //      })
  //      .filter(_._2 <= mindF.value)
  //      .map(_._1)
  //      .collect
  //      .toSet
  //    val samples_bc = sc.broadcast(samples)
  //    val sampleFilteredRdd = genotype.filter(gs => samples_bc.value contains gs.sampleId)
  //
  //    sampleFilteredRdd
  //  }

  //  def filterVariants(genotypeStates: DataFrame, geno: Option[Double], maf: Option[Double]): DataFrame = {
  //    val genoF = sc.broadcast(geno)
  //    val mafF = sc.broadcast(maf)
  //
  //    val genos = genotypeStates.map(gs => (gs.contigName, gs.missingGenotypes, gs.genotypeState, 2))
  //      .keyBy(_._1)
  //      .reduceByKey((tup1, tup2) => (tup1._1, tup1._2 + tup2._2, tup1._3 + tup2._3, tup1._4 + tup2._4))
  //      .map(keyed_tup => {
  //        val (key, tuple) = keyed_tup
  //        val (contigName, missCount, alleleCount, total) = tuple
  //        val geno = missCount.toDouble / total.toDouble
  //        val maf = alleleCount.toDouble / (total - missCount).toDouble
  //        (contigName, geno, maf, 1.0 - maf)
  //      })
  //      .filter(stats => stats._2 <= genoF.value && stats._3 >= mafF.value && stats._4 >= mafF.value)
  //      .map(_._1)
  //      .collect
  //      .toSet
  //    val genos_bc = sc.broadcast(genos)
  //    val genoFilteredRdd = genotypeStates.filter(gs => genos_bc.value contains gs.contigName)
  //
  //    genoFilteredRdd
  //  }

  def loadGenotypes(genotypesPath: String): Dataset[CalledVariant] = {

    val stringVariantDS = sparkSession.read.textFile(genotypesPath).filter(row => !row.startsWith("##"))

    val variantDF = sparkSession.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", "\t")
      .csv(stringVariantDS)

    // drop the variant level metadata
    val samples = variantDF.schema.fields.drop(9).map(x => x.name)

    val genotypeStateConverter: String => GenotypeState = GenotypeState(_)
    val converterUDF = udf(genotypeStateConverter)

    val columnsTomap = variantDF.select(samples.head, samples.tail: _*).columns

    var tempdf = variantDF
    columnsTomap.map(column => { tempdf = tempdf.withColumn(column, converterUDF(col(column))) })

    val formattedVariantDS = tempdf.withColumn("samples", array(samples.head, samples.tail: _*))
      .drop(samples: _*)
      .toDF("chromosome",
        "position",
        "uniqueID",
        "referenceAllele",
        "alternateAllele",
        "qualityScore",
        "filter",
        "info",
        "format",
        "samples")

    formattedVariantDS.as[CalledVariant]
  }

  /**
   *
   * @note assume all covarNames are in covariate file
   * @note assume phenoName is in phenotype file
   * @note assume that
   *
   *
   * @param phenotypesPath
   * @param phenoName
   * @param oneTwo
   * @param delimiter
   * @param includeCovariates
   * @param covarPath
   * @param covarNames
   * @return
   */
  def loadPhenotypes(phenotypesPath: String,
                     primaryID: String,
                     phenoName: String,
                     delimiter: String,
                     covarPath: Option[String] = None,
                     covarNames: Option[List[String]] = None): Dataset[BetterPhenotype] = {

    logInfo("Loading phenotypes from %s.".format(phenotypesPath))

    // ToDo: keeps these operations on one machine, because phenotypes are small.
    val phenotypesDF = sparkSession.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", delimiter)
      .load(phenotypesPath)
      .select(primaryID, phenoName)
      .toDF("sampleId", "phenotype")
      .coalesce(1)

    val covariateDF = if (covarPath.isDefined) {
      Option(
        sparkSession.read.format("csv")
          .option("header", "true")
          .option("inferSchema", "true")
          .option("delimiter", delimiter)
          .load(covarPath.get)
          .select(primaryID, covarNames.get: _*)
          .toDF("sampleId" :: covarNames.get: _*)
          .coalesce(1))
    } else {
      None
    }

    val phenoCovarDF = if (covariateDF.isDefined) {
      val joinedDF = phenotypesDF.join(covariateDF.get, Seq("sampleId"))
      joinedDF.withColumn("covariates", array(covarNames.get.head, covarNames.get.drop(1): _*))
        .select("sampleId", "phenotype", "covariates")
    } else {
      phenotypesDF.withColumn("covariates", lit(null).cast(ArrayType(DoubleType)))
    }

    phenoCovarDF.as[BetterPhenotype]

    //    phenoCovarDF.as[BetterPhenotype]

    //    phenoCovarDF.collect.map(row => {
    //      val rowList = row.toSeq.toList
    //      val phenos = rowList.drop(1).map(x => parseInt.toDouble).toArray
    //      Phenotype(phenoName, rowList.head.asInstanceOf[String], phenos)
    //    }).toList

    //    phenoCovarDF.withColumn("phenotypes", Phenotype(
    //      phenoName,
    //      phenoCovarDF("IID"),
    //      phenoCovarDF(phenoName),
    //      List)
    //    require(phenoHeader.length >= 2,
    //      s"Phenotype files must have a minimum of 2 columns. The first column is a sampleID, " +
    //        "the rest are phenotype values. The first row must be a header that contains labels.")
    //    require(phenoHeader.contains(phenoName),
    //      s"The primary phenotype, '$phenoName' does not exist in the specified file, '$phenotypesPath'")

    //    var covariatesRDD: Option[RDD[String]] = None
    //    var covariateHeader: Option[Array[String]] = None
    //    var covariateIndices: Option[Array[Int]] = None
    //
    //    if (covarPath.isDefined) {
    //      logInfo("Loading covariates from %s.".format(covarPath.get))
    //      covariatesRDD = Some(sc.textFile(covarPath.get).persist())
    //      var covariateNames = covarNames.get.split(",")
    //
    //      covariateHeader = Some(covariatesRDD.get.first().split('\t'))
    //      var delimiter = "\t"
    //
    //      if (covariateHeader.get.length < 2) {
    //        covariateHeader = Some(covariatesRDD.get.first().split(" "))
    //        delimiter = " "
    //      }
    //
    //      require(covariateNames.forall(name => covariateHeader.get.contains(name)),
    //        "One of the covariates specified is missing from the covariate file '%s'".format(covarPath.get))
    //      require(!covariateNames.contains(phenoName), "The primary phenotype cannot be a covariate.")
    //
    //      covariateIndices = Some(covariateNames.map(name => covariateHeader.get.indexOf(name)))
    //    }
    //
    //    combineAndFilterPhenotypes(
    //      oneTwo,
    //      phenotypesRDD,
    //      phenoHeader,
    //      phenoHeader.indexOf(phenoName),
    //      delimiter,
    //      covariatesRDD,
    //      covariateHeader,
    //      covariateIndices)
  }

  //  /**
  //   * Loads in and filters the phenotype and covariate values from the specified files.
  //   *
  //   * @param oneTwo if True, binary phenotypes are encoded as 1 negative response, 2 positive response
  //   * @param phenotypes RDD of the phenotype file read from HDFS, stored as strings
  //   * @param covars RDD of the covars file read from HDFS, stored as strings
  //   * @param splitHeader phenotype file header string
  //   * @param splitCovarHeader covar file header string
  //   * @param primaryPhenoIndex index into the phenotype rows for the primary phenotype
  //   * @param covarIndices indices of the covariates in the covariate rows
  //   * @return RDD of [[Phenotype]] objects that contain the phenotypes and covariates from the specified files
  //   */
  //  private[gnocchi] def combineAndFilterPhenotypes(oneTwo: Boolean,
  //                                                  phenotypes: DataFrame,
  //                                                  splitHeader: Array[String],
  //                                                  delimiter: String,
  //                                                  covars: Option[RDD[String]] = None,
  //                                                  splitCovarHeader: Option[Array[String]] = None,
  //                                                  covarIndices: Option[Array[Int]] = None): RDD[Phenotype] = {
  //
  //    // ToDo: There are better and more clear ways to construct the phenotypes here.
  //    // TODO: NEED TO REQUIRE THAT ALL THE PHENOTYPES BE REPRESENTED BY NUMBERS.
  //
  //    val fullHeader = if (splitCovarHeader.isDefined) splitHeader ++ splitCovarHeader.get else splitHeader
  //
  //    val indices = if (covarIndices.isDefined) {
  //      val mergedIndices = covarIndices.get.map(elem => { elem + splitHeader.length })
  //      List(primaryPhenoIndex) ++ mergedIndices
  //    } else List(primaryPhenoIndex)
  //
  //    val data = phenotypes.filter(line => line != splitHeader.mkString(delimiter))
  //      .map(line => line.split(delimiter))
  //      .keyBy(splitLine => splitLine(0))
  //      .filter(_._1 != "")
  //
  //    val joinedData = if (covars.isDefined) {
  //      val covarData = covars.get.filter(line => line != splitCovarHeader.get.mkString(delimiter))
  //        .map(line => line.split(delimiter))
  //        .keyBy(splitLine => splitLine(0))
  //        .filter(_._1 != "")
  //      data.cogroup(covarData).map(pair => {
  //        val (_, (phenosIterable, covariatesIterable)) = pair
  //        val phenoArray = phenosIterable.head
  //        val covarArray = covariatesIterable.head
  //        phenoArray ++ covarArray
  //      })
  //    } else {
  //      data.map(p => p._2)
  //    }
  //
  //    val finalData = joinedData
  //      .filter(p => p.length > 2)
  //      .filter(p => !indices.exists(index => isMissing(p(index))))
  //      .map(p => if (oneTwo) p.updated(primaryPhenoIndex, (p(primaryPhenoIndex).toDouble - 1).toString) else p)
  //      .map(p => Phenotype(
  //        indices.map(index => fullHeader(index)).mkString(","), p(0), p.head.toDouble, indices.map(i => p(i).toDouble)))
  //
  //    phenotypes.unpersist()
  //    if (covars.isDefined) {
  //      covars.get.unpersist()
  //    }
  //
  //    finalData
  //  }
  //
  //  /**
  //   * Checks if a phenotype value matches the missing value string
  //   *
  //   * @param value value to check
  //   * @return true if the value matches the missing value string, false else
  //   */
  //  private[gnocchi] def isMissing(value: String): Boolean = {
  //    try {
  //      value.toDouble == -9.0
  //    } catch {
  //      case e: java.lang.NumberFormatException => true
  //    }
  //  }
  //
  //  /**
  //   * Generates an RDD of observations that can be used to create or update VariantModels.
  //   *
  //   * @param genotypes RDD of GenotypeState objects
  //   * @param phenotypes RDD of Pheontype objects
  //   * @return Returns an RDD of arrays of observations (genotype + phenotype), keyed by variant
  //   */
  //  def generateObservations(genotypes: RDD[Genotype],
  //                           phenotypes: RDD[Phenotype]): RDD[(Variant, Array[(Double, Array[Double])])] = {
  //    // convert genotypes and phenotypes into observations
  //    val data = pairSamplesWithPhenotypes(genotypes, phenotypes)
  //    // data is RDD[((Variant, String), Iterable[(String, (GenotypeState,Phenotype))])]
  //    data.map(kvv => {
  //      val (varStr, genPhenItr) = kvv
  //      val (variant, phenoName) = varStr
  //      //.asInstanceOf[Array[(String, (GenotypeState, Phenotype[Array[Double]]))]]
  //      val obs = genPhenItr.map(gp => {
  //        val (str, (gs, pheno)) = gp
  //        val ob = (gs, pheno.value)
  //        ob
  //      }).toArray
  //      (variant, obs).asInstanceOf[(Variant, Array[(Double, Array[Double])])]
  //    })
  //  }
  //
  //  /**
  //   *
  //   * @param genotypes  an rdd of [[net.fnothaft.gnocchi.primitives.genotype.Genotype]] objects to be regressed upon
  //   * @param phenotypes an rdd of [[net.fnothaft.gnocchi.primitives.phenotype.Phenotype]] objects used as observations
  //   * @param clipOrKeepState
  //   * @return
  //   */
  //  def formatObservations(genotypes: RDD[Genotype],
  //                         phenotypes: RDD[Phenotype],
  //                         clipOrKeepState: Genotype => Double): RDD[((Variant, String, Int), Array[(Double, Array[Double])])] = {
  //    val joinedGenoPheno = genotypes.keyBy(_.sampleId).join(phenotypes.keyBy(_.sampleId))
  //
  //    val keyedGenoPheno = joinedGenoPheno.map(keyGenoPheno => {
  //      val (_, genoPheno) = keyGenoPheno
  //      val (gs, pheno) = genoPheno
  //      val variant = Variant.newBuilder()
  //        .setContigName(gs.contigName)
  //        .setStart(gs.start)
  //        .setEnd(gs.end)
  //        .setAlternateAllele(gs.alt)
  //        .build()
  //      //        .setNames(Seq(gs.contigName).toList).build
  //      //      variant.setFiltersFailed(List(""))
  //      ((variant, pheno.phenotype, gs.phaseSetId), genoPheno)
  //    })
  //      .groupByKey()
  //
  //    keyedGenoPheno.map(site => {
  //      val ((variant, pheno, phaseSetId), observations) = site
  //      val formattedObs = observations.map(p => {
  //        val (genotypeState, phenotype) = p
  //        (clipOrKeepState(genotypeState), phenotype.toDouble)
  //      }).toArray
  //      ((variant, pheno, phaseSetId), formattedObs)
  //    })
  //  }
  //
  //  def extractQCPhaseSetIds(genotypeStates: RDD[Genotype]): RDD[(Int, String)] = {
  //    genotypeStates.map(g => (g.phaseSetId, g.contigName)).reduceByKey((a, b) => a)
  //  }
  //
  //  def pairSamplesWithPhenotypes(rdd: RDD[Genotype],
  //                                phenotypes: RDD[Phenotype]): RDD[((Variant, String), Iterable[(String, (Genotype, Phenotype))])] = {
  //    rdd.keyBy(_.sampleId)
  //      // join together the samples with both genotype and phenotype entry
  //      .join(phenotypes.keyBy(_.sampleId))
  //      .map(kvv => {
  //        // unpack the entry of the joined rdd into id and actual info
  //        val (sampleid, gsPheno) = kvv
  //        // unpack the information into genotype state and pheno
  //        val (gs, pheno) = gsPheno
  //
  //        // create contig and Variant objects and group by Variant
  //        // pack up the information into an Association object
  //        val variant = gs2variant(gs)
  //        ((variant, pheno.phenotype), (sampleid, gsPheno))
  //      }).groupByKey
  //  }
  //
  //  def gs2variant(gs: Genotype): Variant = {
  //    val variant = new Variant()
  //    val contig = new Contig()
  //    contig.setContigName(gs.contigName)
  //    variant.setStart(gs.start)
  //    variant.setEnd(gs.end)
  //    variant.setAlternateAllele(gs.alt)
  //    variant
  //  }
  //
  //  def loadGnocchiModel(gnocchiModelPath: String) = {
  //    val vmLocation = gnocchiModelPath + "/variantModels"
  //    val qcModelsLocation = gnocchiModelPath + "/qcModels"
  //    val metaDataLocation = gnocchiModelPath + "/metaData"
  //
  //    import AuxEncoders._
  //
  //    val metaData = fromFile(metaDataLocation).mkString.unpickle[GnocchiModelMetaData]
  //
  //    val model = metaData.modelType match {
  //      case "ADDITIVE_LINEAR" => {
  //        val variantModels = sparkSession.read.parquet(vmLocation).as[AdditiveLinearVariantModel].rdd
  //        val qcModels = sparkSession.read.parquet(qcModelsLocation).as[QualityControlVariantModel[AdditiveLinearVariantModel]].rdd
  //          .map(qcv => {
  //            (qcv.variantModel, qcv.observations)
  //          })
  //        AdditiveLinearGnocchiModel(metaData, variantModels, qcModels)
  //      }
  //      case "DOMINANT_LINEAR" => {
  //        val variantModels = sparkSession.read.parquet(vmLocation).as[DominantLinearVariantModel].rdd
  //        val qcModels = sparkSession.read.parquet(qcModelsLocation).as[QualityControlVariantModel[DominantLinearVariantModel]].rdd
  //          .map(qcv => {
  //            (qcv.variantModel, qcv.observations)
  //          })
  //        DominantLinearGnocchiModel(metaData, variantModels, qcModels)
  //      }
  //      case "ADDITIVE_LOGISTIC" => {
  //        val variantModels = sparkSession.read.parquet(vmLocation).as[AdditiveLogisticVariantModel].rdd
  //        val qcModels = sparkSession.read.parquet(qcModelsLocation).as[QualityControlVariantModel[AdditiveLogisticVariantModel]].rdd
  //          .map(qcv => {
  //            (qcv.variantModel, qcv.observations)
  //          })
  //        AdditiveLogisticGnocchiModel(metaData, variantModels, qcModels)
  //      }
  //      case "DOMINANT_LOGISTIC" => {
  //        val variantModels = sparkSession.read.parquet(vmLocation).as[DominantLogisticVariantModel].rdd
  //        val qcModels = sparkSession.read.parquet(qcModelsLocation).as[QualityControlVariantModel[DominantLogisticVariantModel]].rdd
  //          .map(qcv => {
  //            (qcv.variantModel, qcv.observations)
  //          })
  //        DominantLogisticGnocchiModel(metaData, variantModels, qcModels)
  //      }
  //    }
  //    model
  //  }
}