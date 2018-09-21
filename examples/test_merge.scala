import org.bdgenomics.gnocchi.sql.GnocchiSession._
import org.bdgenomics.gnocchi.algorithms.siteregression.LinearSiteRegression
import org.bdgenomics.gnocchi.sql.LinearAssociationsDatasetBuilder


val genos1 = sc.loadGenotypes("./testData/merge/time_genos_1.vcf", "time_genos_1", "ADDITIVE")
val phenos1 = sc.loadPhenotypes("./testData/merge/time_phenos_1.txt", "IID", "pheno_1", "\t", Option("./testData/merge/time_phenos_1.txt"), Option(List("pheno_2", "pheno_3")), "\t")
val linearResults1 = LinearSiteRegression(genos1, phenos1)

val genos2 = sc.loadGenotypes("./testData/merge/time_genos_2.vcf", "time_genos_2", "ADDITIVE")
val phenos2 = sc.loadPhenotypes("./testData/merge/time_phenos_2.txt", "IID", "pheno_1", "\t", Option("./testData/merge/time_phenos_2.txt"), Option(List("pheno_2", "pheno_3")), "\t")
val linearResults2 = LinearSiteRegression(genos2, phenos2)

val model1 = linearResults1.gnocchiModel
val model2 = linearResults2.gnocchiModel

val merged = model1.mergeGnocchiModel(model2)

val builder = LinearAssociationsDatasetBuilder(merged, genos1, phenos1)

val updatedBuilder = builder.addNewData(genos2, phenos2)

val mergedGenos = sc.loadGenotypes("./testData/merge/merged.vcf", "merged_vcf", "ADDITIVE")
val mergedPhenos = sc.loadPhenotypes("./testData/merge/merged_phenos.txt", "IID", "pheno_1", "\t", Option("./testData/merge/merged_phenos.txt"), Option(List("pheno_2", "pheno_3")), "\t")
val mergedAssociations = LinearSiteRegression(mergedGenos, mergedPhenos)

val sortedManualMergeAssociations = updatedBuilder.linearAssociationBuilders.map(_.association).sort($"pValue".asc)
val sortedMergeAssociations = mergedAssociations.associations.sort($"pValue".asc)

