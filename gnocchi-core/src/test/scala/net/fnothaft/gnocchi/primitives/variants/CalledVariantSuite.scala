package net.fnothaft.gnocchi.primitives.variants

import net.fnothaft.gnocchi.GnocchiFunSuite

class CalledVariantSuite extends GnocchiFunSuite {

  // minor allele frequency tests
  sparkTest("CalledVariant.maf should return the frequency rate of the minor allele: minor allele is actually the major allele") {
    // create a called variant object where the alt has a frequency of 70%, maf should return 0.7
    val calledVar = createSampleCalledVariant(samples = Option(createSampleGenotypeStates(num = 100, maf = 0.7, geno = 0)))
    assert(calledVar.maf == 0.7, "CalledVariant.maf does not calculate Minor Allele frequency correctly when the specified alternate allele has a higher frequency than the reference allele.")
  }

  sparkTest("CalledVariant.maf should return the frequency rate of the minor allele: no minor alleles present.") {
    // create a called variant object where there are no alts.
    val calledVar = createSampleCalledVariant(samples = Option(createSampleGenotypeStates(num = 100, maf = 0, geno = 0)))
    assert(calledVar.maf == 0.0, "CalledVariant.maf does not calculate Minor Allele frequency correctly when there are no minor alleles present.")
  }

  sparkTest("CalledVariant.maf should return the frequency rate of the minor allele: only minor alleles present.") {
    val calledVar = createSampleCalledVariant(samples = Option(createSampleGenotypeStates(num = 100, maf = 1.0, geno = 0)))
    assert(calledVar.maf == 1.0, "CalledVariant.maf does not calculate Minor Allele frequency correctly when there are no minor alleles present.")
  }

  sparkTest("CalledVariant.maf should return the frequency rate of the minor allele: all values are missing.") {
    val calledVar = createSampleCalledVariant(samples = Option(createSampleGenotypeStates(num = 100, maf = 0.0, geno = 1.0)))
    try {
      calledVar.maf
      fail("CalledVariant.maf should throw an assertion error when there are all missing values.")
    } catch {
      case e: java.lang.AssertionError =>
    }
  }

  // genotyping rate tests

  sparkTest("CalledVariant.geno should return the fraction of missing values: all values are missing.") {
    val calledVar = createSampleCalledVariant(samples = Option(createSampleGenotypeStates(num = 100, maf = 0.0, geno = 1.0)))
    assert(calledVar.geno == 1.0, "CalledVariant.geno incorrectly calculates genotyping missingness when all values are missing.")
  }

  sparkTest("CalledVariant.geno should return the fraction of missing values: no values are missing.") {
    val calledVar = createSampleCalledVariant(samples = Option(createSampleGenotypeStates(num = 100, maf = 0.0, geno = 0.0)))
    assert(calledVar.geno == 0.0, "CalledVariant.geno incorrectly calculates genotyping missingness when nos values are missing.")
  }

  // num valid samples tests

  ignore("CalledVariant.numValidSamples should only count samples with no missing values") {

  }

  ignore("CalledVariant.numValidSamples should return an Int") {

  }

  // num semi valid samples tests

  ignore("CalledVariant.numSemiValidSamples should count the number of samples that have some valid values.") {

  }

  ignore("CalledVariant.numSemiValidSamples should return an Int") {

  }
}
