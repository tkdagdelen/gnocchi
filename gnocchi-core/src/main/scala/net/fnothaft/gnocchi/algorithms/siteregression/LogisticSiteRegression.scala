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
package net.fnothaft.gnocchi.algorithms.siteregression

import breeze.linalg._
import breeze.numerics.{ log10, _ }
import net.fnothaft.gnocchi.models.variant.VariantModel
import net.fnothaft.gnocchi.models.variant.logistic.AdditiveLogisticVariantModel
import net.fnothaft.gnocchi.rdd.association.{ AdditiveLogisticAssociation, Association, DominantLogisticAssociation }
import org.apache.commons.math3.distribution.ChiSquaredDistribution
import org.apache.spark.mllib.regression.LabeledPoint
import org.bdgenomics.formats.avro.Variant

trait LogisticSiteRegression[VM <: VariantModel[VM]] extends SiteApplication[VM] {

  /**
   * This method will perform logistic regression on a single site.
   *
   * @param observations An array containing tuples in which the first element is the coded genotype. The second is an Array[Double] representing the phenotypes, where the first element in the array is the phenotype to regress and the rest are to be treated as covariates. .
   * @param variant The variant that is being regressed.
   * @param phenotype    The name of the phenotype being regressed.
   * @return The Association object that results from the linear regression
   */

  def applyToSite(observations: Array[(Double, Array[Double])],
                  variant: Variant,
                  phenotype: String): Association[VM] = {

    // transform the data in to design matrix and y matrix compatible with mllib's logistic regresion
    val observationLength = observations(0)._2.length
    val numObservations = observations.length
    val lp = new Array[LabeledPoint](numObservations)
    val xixiT = new Array[DenseMatrix[Double]](numObservations)
    val xiVectors = new Array[DenseVector[Double]](numObservations)

    // iterate over observations, copying correct elements into sample array and filling the x matrix.
    // the first element of each sample in x is the coded genotype and the rest are the covariates.
    var features = new Array[Double](observationLength)
    for (i <- observations.indices) {
      // rearrange variables into label and features
      features = new Array[Double](observationLength)
      features(0) = observations(i)._1.toDouble
      observations(i)._2.slice(1, observationLength).copyToArray(features, 1)
      val label = observations(i)._2(0)

      // pack up info into LabeledPoint object
      lp(i) = new LabeledPoint(label, new org.apache.spark.mllib.linalg.DenseVector(features))

      // compute xi*xi.t for hessian matrix
      val xiVector = DenseVector(1.0 +: features)
      xiVectors(i) = xiVector
      xixiT(i) = xiVector * xiVector.t
    }

    // initialize parameters
    var iter = 0
    val maxIter = 1000
    val tolerance = 1e-6
    var singular = false
    var convergence = false
    var update: DenseVector[Double] = DenseVector[Double]()
    val beta = Array.fill[Double](observationLength + 1)(0.0)
    val data = lp
    var pi = 0.0
    var hessian = DenseMatrix.zeros[Double](observationLength + 1, observationLength + 1)

    // optimize using Newton-Raphson
    while ((iter < maxIter) && !convergence && !singular) {
      try {
        // calculate the logit for each xi
        val logitArray = logit(data, beta)

        // calculate the hessian and score
        hessian = DenseMatrix.zeros[Double](observationLength + 1, observationLength + 1)
        var score = DenseVector.zeros[Double](observationLength + 1)
        for (i <- observations.indices) {
          pi = Math.exp(-logSumOfExponentials(Array(0.0, -logitArray(i))))
          hessian += -xixiT(i) * pi * (1.0 - pi)
          score += xiVectors(i) * (lp(i).label - pi)
        }

        // compute the update and check convergence
        update = -inv(hessian) * score
        if (max(abs(update)) <= tolerance) {
          convergence = true
        }

        // compute new weights
        for (j <- beta.indices) {
          beta(j) += update(j)
        }

        if (beta.exists(_.isNaN)) {
          // TODO: Log this instead of printing it
          println("LOG_REG - Broke on iteration: " + iter)
          iter = maxIter
        }
      } catch {
        case error: breeze.linalg.MatrixSingularException => {
          singular = true
        }
      }
      iter += 1
    }

    /// CALCULATE WALD STATISTIC "P Value" ///

    // calculate the standard error for the genotypic predictor
    var matrixSingular = false

    try {
      val fisherInfo = -hessian
      val fishInv = inv(fisherInfo)
      val standardErrors = sqrt(abs(diag(fishInv)))
      val genoStandardError = standardErrors(1)

      // calculate Wald statistic for each parameter in the regression model
      val zScores: DenseVector[Double] = DenseVector(beta) :/ standardErrors
      val waldStats = zScores :* zScores

      // calculate cumulative probs
      val chiDist = new ChiSquaredDistribution(1) // 1 degree of freedom
      val probs = waldStats.map(zi => {
        chiDist.cumulativeProbability(zi)
      })

      // calculate wald test statistics
      val waldTests = 1d - probs

      // calculate the log of the p-value for the genetic component
      val logWaldTests = waldTests.map(t => {
        log10(t)
      })

      val statistics = Map("numSamples" -> numObservations,
        "weights" -> beta,
        "standardError" -> genoStandardError,
        "intercept" -> beta(0),
        "'P Values' aka Wald Tests" -> waldTests,
        "log of wald tests" -> logWaldTests,
        "fisherInfo" -> fisherInfo,
        "XiVectors" -> xiVectors(0),
        "xixit" -> xixiT(0),
        "prob" -> pi,
        "rSquared" -> 0.0)

      constructAssociation(variant.getContig.getContigName,
        numObservations,
        "Logistic",
        beta,
        genoStandardError,
        variant,
        phenotype,
        waldTests(1),
        logWaldTests(1),
        statistics)
    } catch {
      case error: breeze.linalg.MatrixSingularException => {
        matrixSingular = true
        constructAssociation(variant.getContig.getContigName,
          numObservations,
          "Logistic",
          beta,
          1.0,
          variant,
          phenotype,
          0.0,
          0.0,
          Map(
            "numSamples" -> 0,
            "weights" -> beta,
            "intercept" -> 0.0,
            "'P Values' aka Wald Tests" -> 0.0,
            "log of wald tests" -> 0.0,
            "fisherInfo" -> 0.0,
            "XiVectors" -> xiVectors(0),
            "xixit" -> xixiT(0),
            "prob" -> pi,
            "rSquared" -> 0.0))
      }
    }
  }

  def logit(lpArray: Array[LabeledPoint], b: Array[Double]): Array[Double] = {
    val logitResults = new Array[Double](lpArray.length)
    val bDense = DenseVector(b)
    for (j <- logitResults.indices) {
      val lp = lpArray(j)
      logitResults(j) = DenseVector(1.0 +: lp.features.toArray) dot bDense
    }
    logitResults
  }

  def logSumOfExponentials(exps: Array[Double]): Double = {
    if (exps.length == 1) {
      exps(0)
    }
    val maxExp = max(exps)
    var sums = 0.0
    for (i <- exps.indices) {
      if (exps(i) != 1.2340980408667956E-4) {
        sums += java.lang.Math.exp(exps(i) - maxExp)
      }
    }
    maxExp + Math.log(sums)
  }

  def constructAssociation(variantId: String,
                           numSamples: Int,
                           modelType: String,
                           weights: Array[Double],
                           geneticParameterStandardError: Double,
                           variant: Variant,
                           phenotype: String,
                           logPValue: Double,
                           pValue: Double,
                           statistics: Map[String, Any]): Association[VM]
}

object AdditiveLogisticRegression extends AdditiveLogisticRegression {
  val regressionName = "additiveLogisticRegression"
}
trait AdditiveLogisticRegression extends LogisticSiteRegression[AdditiveLogisticVariantModel] with Additive {
  def constructAssociation(variantId: String,
                           numSamples: Int,
                           modelType: String,
                           weights: Array[Double],
                           geneticParameterStandardError: Double,
                           variant: Variant,
                           phenotype: String,
                           logPValue: Double,
                           pValue: Double,
                           statistics: Map[String, Any]): AdditiveLogisticAssociation = {
    new AdditiveLogisticAssociation(variantId, numSamples, modelType, weights, geneticParameterStandardError,
      variant, phenotype, logPValue, pValue, statistics)
  }
}

object DominantLogisticRegression extends AdditiveLogisticRegression {
  val regressionName = "dominantLogisticRegression"
}
trait DominantLogisticRegression extends LogisticSiteRegression[AdditiveLogisticVariantModel] with Dominant {
  def constructAssociation(variantId: String,
                           numSamples: Int,
                           modelType: String,
                           weights: Array[Double],
                           geneticParameterStandardError: Double,
                           variant: Variant,
                           phenotype: String,
                           logPValue: Double,
                           pValue: Double,
                           statistics: Map[String, Any]): DominantLogisticAssociation = {
    new DominantLogisticAssociation(variantId, numSamples, modelType, weights, geneticParameterStandardError,
      variant, phenotype, logPValue, pValue, statistics)
  }
}