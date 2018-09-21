package org.bdgenomics.gnocchi.cli

import org.apache.spark.SparkContext
import org.bdgenomics.gnocchi.sql.GnocchiSession._
import org.bdgenomics.utils.cli.{ Args4j, Args4jBase, BDGCommandCompanion, BDGSparkCommand }
import org.kohsuke.args4j.Argument

object MergeLinearModels extends BDGCommandCompanion {
  val commandName = "mergeLinearGnocchiModels"
  val commandDescription = "Merge two LinearGnocchiModels and save the result."

  def apply(cmdLine: Array[String]) = {
    new MergeLinearModels(Args4j[MergeLinearModelsArgs](cmdLine))
  }
}

class MergeLinearModelsArgs extends Args4jBase {
  @Argument(required = true, metaVar = "MODEL_1", usage = "The path to the first model to be merged.", index = 0)
  var model1: String = _

  @Argument(required = true, metaVar = "MODEL_2", usage = "The path to the second model to be merged.", index = 1)
  var model2: String = _

  @Argument(required = true, metaVar = "OUTPUT", usage = "The path to save the merged model.", index = 2)
  var output: String = _

}

class MergeLinearModels(protected val args: MergeLinearModelsArgs) extends BDGSparkCommand[MergeLinearModelsArgs] {
  val companion = MergeLinearModels

  def run(sc: SparkContext) {
    val model1 = sc.loadLinearGnocchiModel(args.model1)
    val model2 = sc.loadLinearGnocchiModel(args.model2)

    val merged = model1.mergeGnocchiModel(model2)
    merged.save(args.output)
  }
}
