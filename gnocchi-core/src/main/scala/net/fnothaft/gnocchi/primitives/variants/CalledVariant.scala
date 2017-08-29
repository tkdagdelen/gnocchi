package net.fnothaft.gnocchi.primitives.variants

import net.fnothaft.gnocchi.primitives.genotype.GenotypeState

case class CalledVariant(chromosome: Int,
                         position: Int,
                         uniqueID: String,
                         referenceAllele: String,
                         alternateAllele: String,
                         qualityScore: Int,
                         filter: String,
                         info: String,
                         format: String,
                         samples: List[GenotypeState]) extends Product