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
import org.apache.spark.sql.functions.{ concat, lit, when, array, typedLit, udf, col, sum }
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

  //  private def toGenotypeStateDataFrame(gtFrame: DataFrame, ploidy: Int): DataFrame = {
  //    // generate expression
  //    val genotypeState = (0 until ploidy).map(i => {
  //      val c: Column = when(gtFrame("alleles").getItem(i) === "REF", 1).otherwise(0)
  //      c
  //    }).reduce(_ + _)
  //
  //    val missingGenotypes = (0 until ploidy).map(i => {
  //      val c: Column = when(gtFrame("alleles").getItem(i) === "NO_CALL", 1).otherwise(0)
  //      c
  //    }).reduce(_ + _)
  //
  //    // is this correct? or should we change the column to nullable?
  //    val phaseSetId: Column = when(gtFrame("phaseSetId").isNull, 0).otherwise(gtFrame("phaseSetId"))
  //
  //    gtFrame.select(gtFrame("variant.contigName").as("contigName"),
  //      gtFrame("variant.start").as("start"),
  //      gtFrame("variant.end").as("end"),
  //      gtFrame("variant.referenceAllele").as("ref"),
  //      gtFrame("variant.alternateAllele").as("alt"),
  //      gtFrame("sampleId"),
  //      genotypeState.as("genotypeState"),
  //      missingGenotypes.as("missingGenotypes"),
  //      phaseSetId.as("phaseSetId"))
  //  }

  def filterSamples(genotypes: Dataset[CalledVariant], mind: Double, ploidy: Double): Dataset[CalledVariant] = {
    val sampleIds = genotypes.first.samples.map(x => x.sampleID)
    val separated = genotypes.select($"uniqueID" +: sampleIds.indices.map(idx => $"samples"(idx) as sampleIds(idx)): _*)

    val missingFn: String => Int = _.split("/|\\|").count(_ == ".")
    val missingUDF = udf(missingFn)

    val filtered = separated.select($"uniqueID" +: sampleIds.map(sampleId => missingUDF(separated(sampleId).getField("value")) as sampleId): _*)

    val summed = filtered.drop("uniqueID").groupBy().sum().toDF(sampleIds: _*)
    val count = filtered.count()

    val missingness = summed.select(sampleIds.map(sampleId => summed(sampleId) / (ploidy * count) as sampleId): _*)
    val plainMissingness = missingness.select(array(sampleIds.head, sampleIds.tail: _*)).as[Array[Double]].head

    val samplesWithMissingness = sampleIds.zip(plainMissingness)
    val keepers = samplesWithMissingness.filter(x => x._2 <= mind).map(x => x._1)

    val filteredDF = separated.select($"uniqueID", array(keepers.head, keepers.tail: _*)).toDF("uniqueID", "samples").as[(String, Array[String])]

    genotypes.drop("samples").join(filteredDF, "uniqueID").as[CalledVariant]
  }

  def filterVariants(genotypes: Dataset[CalledVariant], geno: Double, maf: Double): Dataset[CalledVariant] = {
    def filtersDF = genotypes.map(x => (x.uniqueID, x.maf, x.geno)).toDF("uniqueID", "maf", "geno")
    def mafFiltered = genotypes.join(filtersDF, "uniqueID")
      .filter($"maf" >= maf && lit(1) - $"maf" >= maf && $"geno" <= geno)
      .drop("maf", "geno")
      .as[CalledVariant]

    mafFiltered
  }

  def loadGenotypes(genotypesPath: String): Dataset[CalledVariant] = {

    // ToDo: Deal with multiple Alts
    val stringVariantDS = sparkSession.read.textFile(genotypesPath).filter(row => !row.startsWith("##"))

    val variantDF = sparkSession.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", "\t")
      .csv(stringVariantDS)

    // drop the variant level metadata
    val samples = variantDF.schema.fields.drop(9).map(x => x.name)

    val groupedSamples = variantDF
      .select($"ID", array(samples.head, samples.tail: _*))
      .as[(String, Array[String])]
    val typedGroupedSamples = groupedSamples
      .map(row => (row._1, samples.zip(row._2).map(x => GenotypeState(x._1, x._2))))
      .toDF("ID", "samples")
      .as[(String, Array[GenotypeState])]

    val formattedRawDS = variantDF.drop(samples: _*).join(typedGroupedSamples, "ID")

    val formattedVariantDS = formattedRawDS.toDF(
      "uniqueID",
      "chromosome",
      "position",
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
   * @param covarPath
   * @param covarNames
   * @return
   */
  def loadPhenotypes(phenotypesPath: String,
                     primaryID: String,
                     phenoName: String,
                     delimiter: String,
                     covarPath: Option[String] = None,
                     covarNames: Option[List[String]] = None): Map[String, BetterPhenotype] = {

    logInfo("Loading phenotypes from %s.".format(phenotypesPath))

    // ToDo: keeps these operations on one machine, because phenotypes are small.
    val prelimPhenotypesDF = sparkSession.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", delimiter)
      .load(phenotypesPath)

    val phenoHeader = prelimPhenotypesDF.schema.fields.map(_.name)

    require(phenoHeader.contains(phenoName),
      s"The primary phenotype, '$phenoName' does not exist in the specified file, '$phenotypesPath'")
    require(phenoHeader.contains(primaryID),
      s"The primary sample ID, '$primaryID' does not exist in the specified file, '$phenotypesPath'")

    val phenotypesDF = prelimPhenotypesDF
      .select(primaryID, phenoName)
      .toDF("sampleId", "phenotype")

    val covariateDF = if (covarPath.isDefined) {
      val prelimCovarDF = sparkSession.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("delimiter", delimiter)
        .load(covarPath.get)

      val covarHeader = prelimCovarDF.schema.fields.map(_.name)

      require(covarNames.get.forall(covarHeader.contains(_)),
        s"One of the covariates, '%s' does not exist in the specified file, '%s'".format(covarNames.get.toString(), covarPath.get))
      require(covarHeader.contains(primaryID),
        s"The primary sample ID, '$primaryID' does not exist in the specified file, '%s'".format(covarPath.get))
      require(!covarNames.get.contains(phenoName),
        s"The primary phenotype, '$phenoName' cannot be listed as a covariate. '%s'".format(covarNames.get.toString()))

      Option(prelimCovarDF
        .select(primaryID, covarNames.get: _*)
        .toDF("sampleId" :: covarNames.get: _*))
    } else {
      None
    }

    val phenoCovarDF = if (covariateDF.isDefined) {
      val joinedDF = phenotypesDF.join(covariateDF.get, Seq("sampleId"))
      joinedDF.withColumn("covariates", array(covarNames.get.head, covarNames.get.tail: _*))
        .select("sampleId", "phenotype", "covariates")
    } else {
      phenotypesDF.withColumn("covariates", lit(null).cast(ArrayType(DoubleType)))
    }

    phenoCovarDF.as[BetterPhenotype].collect().map(x => (x.sampleId, x)).toMap
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