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
 */

package net.fnothaft.gnocchi.cli

import java.io.File

import net.fnothaft.gnocchi.GnocchiFunSuite
import java.nio.file.{ Files, Paths }

import net.fnothaft.gnocchi.models.{ AdditiveLogisticGnocchiModel, AdditiveLinearGnocchiModel, VariantModel }

class GnocchiModelSuite extends GnocchiFunSuite {

  //
  val path = "src/test/resources/testData/Association"
  val destination = Files.createTempDirectory("").toAbsolutePath.toString + "/" + path
  val genoFilePath = ClassLoader.getSystemClassLoader.getResource("5snps10samples.vcf").getFile
  val phenoFilePath = ClassLoader.getSystemClassLoader.getResource("10samples5Phenotypes2covars.txt").getFile
  val covarFilePath = ClassLoader.getSystemClassLoader.getResource("10samples5Phenotypes2covars.txt").getFile
  val modelPath = "src/test/resources/testData/GnocchiModel"
  //  val modelDestination = Files.createTempDirectory("").toAbsolutePath.toString + "/" + modelPath
  val baseDir = new File(".").getAbsolutePath

  //  val modelDestination = "File://" + baseDir + modelPath
  val modelDestination = baseDir + "src/test/resources/testData/GnocchiModel"

  sparkTest("Test GnocchiModel save method") {
    val gm = new AdditiveLinearGnocchiModel
    SaveGnocchiModel(gm, modelDestination)
    assert(Files.exists(Paths.get(modelDestination)), "File doesn't exist")
  }

  // Construct the GnocchiModel, save, and check that the saved model exists and has the right information inside.
  sparkTest("Test GnocchiModel construction, saving, loading: 5 snps, 10 samples, 1 phenotype, 2 random noise covars") {
    /* 
     Uniform White Noise for Covar 1 (pheno4): 
      0.8404
     -0.8880
      0.1001
     -0.5445
      0.3035
     -0.6003
      0.4900
      0.7394
      1.7119
     -0.1941
    Uniform White Noise for Covar 2 (pheno5): 
      2.9080
      0.8252
      1.3790
     -1.0582
     -0.4686
     -0.2725
      1.0984
     -0.2779
      0.7015
     -2.0518
   */

    val cliCall = s"../bin/gnocchi-submit ConstructGnocchiModel $genoFilePath $phenoFilePath ADDITIVE_LINEAR $destination -saveAsText -saveModelTo $modelDestination -phenoName pheno1 -covar -covarFile $covarFilePath -covarNames pheno4,pheno5 -overwriteParquet"
    val cliArgs = cliCall.split(" ").drop(2)
    ConstructGnocchiModel(cliArgs).run(sc)
    val loadedModel = LoadGnocchiModel(modelDestination)
    println(loadedModel.variantModels)
    val variantModels = loadedModel.variantModels
    val regressionResult = variantModels.head._2.asInstanceOf[VariantModel].association
    println(regressionResult.statistics)
    assert(regressionResult.statistics("rSquared") === 0.833277921795612, "Incorrect rSquared = " + regressionResult.statistics("rSquared"))

  }

  sparkTest("GnocchiModel construction, saving, loading, updating, re-saving, re-loading: 5 snps, 5 + 5 samples, 1 phenotype, 2 random noise covars") {
//    // create GM on subset of the data
//    val genoFilePath =
//    val phenoFilePath =
//    val ogModelDestination =
//    val cliCall = s"../bin/gnocchi-submit ConstructGnocchiModel $genoFilePath $phenoFilePath ADDITIVE_LINEAR $destination -saveAsText -saveModelTo $ogModelDestination -phenoName pheno1 -covar -covarFile $covarFilePath -covarNames pheno4,pheno5 -overwriteParquet"
//    val cliArgs = cliCall.split(" ").drop(2)
//    ConstructGnocchiModel(cliArgs).run(sc)
//
//    // update GM on the remainder of the data
//    val genosForUpdate =
//    val phenosForUpdate =
//    val modelLocation = ogModelDestination
//    val updatedModelDestination = baseDir + "src/test/resources/testData/UpdatedGnocchiModel"
//    val updateCliCall = s"../bin/gnocchi-submit UpdateGnocchiModel $genoFilePath $phenoFilePath ADDITIVE_LINEAR $destination -saveAsText -modelLocation $modelLocation -saveModelTo $updatedModelDestination -phenoName pheno1 -covar -covarFile $covarFilePath -covarNames pheno4,pheno5 -overwriteParquet"
//
//    // create GM on all of the data
//    val fullRecomputeCliCall = s"../bin/gnocchi-submit ConstructGnocchiModel $genoFilePath $phenoFilePath ADDITIVE_LINEAR $destination -saveAsText -saveModelTo $modelDestination -phenoName pheno1 -covar -covarFile $covarFilePath -covarNames pheno4,pheno5 -overwriteParquet"
//    val fullRecomputeCliArgs = fullRecomputeCliCall.split(" ").drop(2)
//    ConstructGnocchiModel(fullRecomputeCliArgs).run(sc)
//
//    // load in all three models
//    val fullRecomputeModel = LoadGnocchiModel(ogModelDestination)
//    val ogModel = LoadGnocchiModel(ogModelDestination)
//    val updatedModel = LoadGnocchiModel(updatedModelDestination)
//
//    // verify that their numSamples are correct.
//    val fullNumSamples = fullRecomputeModel.numSamples
//    val ogNumSamples = ogModel.numSamples
//    val updatedNumSamples = updatedModel.numSamples
//    assert(updatedNumSamples === 10, "Number of samples in Updated model not consistent with full recompute model")
//    assert(ogNumSamples === 5, "Incorrect number of samples in original model before update.")

    // verify that the updated model's results are close to the full recompute model


  }


  sparkTest("GnocchiModel loading saved model, making predictions, and re-saving: 5 snps, 10 samples, 1 phenotype, 2 random noise covars") {
    // build original model
//    val ogGenoPath =
//    val ogPhenoPath =
//    val ogModelDestination =
//    val ogCovarsPath =
//    val cliCall = s"../bin/gnocchi-submit ConstructGnocchiModel $ogGenoPath $ogPhenoPath ADDITIVE_LINEAR $destination -saveAsText -saveModelTo $ogModelDestination -phenoName pheno1 -covar -covarFile $ogCovarsPath -covarNames pheno4,pheno5 -overwriteParquet"
//    val cliArgs = cliCall.split(" ").drop(2)
//    ConstructGnocchiModel(cliArgs).run(sc)

    // load the model

    // make predictions

    // re-save model
  }

  sparkTest("GnocchiModel loading saved model, testing on new data, and re-saving") {
    // build original model

    // load the model

    // test on new data

    // re-save model

    // load model and check for test scores
  }

  sparkTest("GnocchiModel ensuring that endpoint phenotype is same when updating, or testing.") {
    assert(false)
  }

  sparkTest("GnocchiModel ensuring that number and names of covariates is same when updating or testing.") {
    assert(false)
  }

  sparkTest("GnocchiModel ensuring that type of model in update or test call is same as the model being loaded.") {
    assert(false)
  }

}