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

package net.fnothaft.gnocchi.cli

import java.nio.file.Files
import java.io.File
import org.apache.commons.io.FileUtils
import org.apache.commons.io.filefilter.WildcardFileFilter

import net.fnothaft.gnocchi.GnocchiFunSuite
import net.fnothaft.gnocchi.models.Association

class AnnotatedVCFHandlingSuite extends GnocchiFunSuite {

  val path = "src/test/resources/testData/AnnotatedVCFHandlingSuite"
  val destination = Files.createTempDirectory("").toAbsolutePath.toString + "/" + path

  sparkTest("Processing in annotated VCFfile from snpEff") {
    val genoFilePath = ClassLoader.getSystemClassLoader.getResource("small_snpeff.vcf").getFile
    val phenoFilePath = ClassLoader.getSystemClassLoader.getResource("2Liner_annot.txt").getFile
    val cliCall = s"../bin/gnocchi-submit regressPhenotypes $genoFilePath $phenoFilePath ADDITIVE_LINEAR $destination -saveAsText -phenoName pheno2 -covar -overwriteParquet"
    val cliArgs = cliCall.split(" ").drop(2)

    val genotypeStateDataset = RegressPhenotypes(cliArgs).loadGenotypes(sc)
    val genotypeStateArray = genotypeStateDataset.collect()

    val variantAnnotationRDD = RegressPhenotypes(cliArgs).loadAnnotations(sc)

    println("Printing GSA")
    for (gs <- genotypeStateArray) {
      println(gs)
      println("---")
    }

    assert(genotypeStateArray.length === 15)
    assert(variantAnnotationRDD.count === 5)

    assert(variantAnnotationRDD.first._2.getAncestralAllele === null)
    assert(variantAnnotationRDD.first._2.getAlleleCount === 2)
    assert(variantAnnotationRDD.first._2.getReadDepth === null)
    assert(variantAnnotationRDD.first._2.getForwardReadDepth === null)
    assert(variantAnnotationRDD.first._2.getReverseReadDepth === null)
    assert(variantAnnotationRDD.first._2.getAlleleFrequency === 0.333f)
    assert(variantAnnotationRDD.first._2.getCigar === null)
    assert(variantAnnotationRDD.first._2.getDbSnp === null)
    assert(variantAnnotationRDD.first._2.getHapMap2 === null)
    assert(variantAnnotationRDD.first._2.getHapMap3 === null)
    assert(variantAnnotationRDD.first._2.getValidated === null)
    assert(variantAnnotationRDD.first._2.getThousandGenomes === null)
    assert(variantAnnotationRDD.first._2.getSomatic === false)
    assert(variantAnnotationRDD.first._2.getAttributes.get("ClippingRankSum") == "0.138")

  }

  sparkTest("Joining Annotation and Assocation RDDs") {
    val genoFilePath = ClassLoader.getSystemClassLoader.getResource("small_snpeff.vcf").getFile
    val phenoFilePath = ClassLoader.getSystemClassLoader.getResource("2Liner_annot.txt").getFile
    val cliCall = s"../bin/gnocchi-submit regressPhenotypes $genoFilePath $phenoFilePath ADDITIVE_LINEAR $destination -saveAsText -phenoName pheno2 -overwriteParquet"
    val cliArgs = cliCall.split(" ").drop(2)

    val genotypeStateDataset = RegressPhenotypes(cliArgs).loadGenotypes(sc)
    val phenotypeStateDataset = RegressPhenotypes(cliArgs).loadPhenotypes(sc)
    val variantAnnotationRDD = RegressPhenotypes(cliArgs).loadAnnotations(sc)

    val associations = RegressPhenotypes(cliArgs).performAnalysis(genotypeStateDataset, phenotypeStateDataset, Some(variantAnnotationRDD), sc)

    assert(variantAnnotationRDD.count === 5)

    assert(associations.rdd.first.variant.getContigName === "1_14522_A")
    assert(associations.rdd.first.variant.getAlternateAllele === "A")
    assert(associations.rdd.first.variantAnnotation.isDefined === true)
    assert(associations.rdd.first.variantAnnotation.get.getAlleleCount == 2)
    assert(associations.rdd.first.variantAnnotation.get.getAlleleFrequency == 0.333f)
    assert(associations.rdd.first.variantAnnotation.get.getSomatic == false)
    assert(associations.rdd.first.variantAnnotation.get.getAttributes.get("ClippingRankSum") == "-2.196")

  }

  sparkTest("Annotations being successfully written to output log file") {

    val testOutput = "../test_data_out/annotations_test"

    val genoFilePath = ClassLoader.getSystemClassLoader.getResource("small_snpeff.vcf").getFile
    val phenoFilePath = ClassLoader.getSystemClassLoader.getResource("2Liner_annot.txt").getFile
    val cliCall = s"../bin/gnocchi-submit regressPhenotypes $genoFilePath $phenoFilePath ADDITIVE_LINEAR $testOutput -saveAsText -phenoName pheno1 -overwriteParquet"
    val cliArgs = cliCall.split(" ").drop(2)

    RegressPhenotypes(cliArgs).run(sc)

    // val logFile = scala.io.Source.fromFile(s"$destination").mkString
    // (TODO) Process and verify contents of logFile

    FileUtils.deleteDirectory(new File("../test_data_out"))
  }

}
