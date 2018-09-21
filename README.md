# Gnocchi

[![Coverage Status](https://coveralls.io/repos/github/bigdatagenomics/gnocchi/badge.svg?branch=master)](https://coveralls.io/github/bigdatagenomics/gnocchi?branch=master)

Statistical associations using the [ADAM](https://github.com/bigdatagenomics/adam) genomics analysis platform.
The currently supported operations are Genome Wide Association using Linear and Logistic models with either Dominant or Additive assumptions.

# Distributed Model Building for Linear Genomic Models

Gnocchi allows for the building of a single linear genomic model over multiple distinct datasets that reside in separate locations.
Gnocchi addresses the use case of multiple collaborators having genomic datasets that are too large to transport
efficiently.

When using gnocchi, it is not necessary to move all datasets into a single datacenter. Gnocchi can incrementally build
partial models that can be merged into a single model that is equivalent to one built over a union of all datasets used
in the model merging process. This result is achieved through storage of relevant sufficient statistics which allow for
partial models to be composed at a later time.

The benefits of this approach are that a set of collaborators can each contribute equal share of compute power to their
portion of an analysis and efficiently communicate the results with other collaborators. It also allows for more effective
checkpointing of expensive computation associated with genomic models. Instead of recomputing an entire association a
researcher can now incrementally add additional data to a previously built model.

# Build

To build, install [Maven](http://maven.apache.org). Then (once in the gnocchi directory) run:

```
mvn package
```

Maven will automatically pull down and install all of the necessary dependencies.
Occasionally, building in Maven will fail due to memory issues. You can work around this
by setting the `MAVEN_OPTS` environment variable to `-Xmx2g -XX:MaxPermSize=1g`.

# Run

Gnocchi is built on top of [Apache Spark](http://spark.apache.org). If you are just evaluating locally, you can use
[a prebuilt Spark distribution](http://spark.apache.org/downloads.html). If you'd like to
use a cluster, refer to Spark's [cluster overview](http://spark.apache.org/docs/latest/cluster-overview.html).

Once Spark is installed, set the environment variable `SPARK_HOME` to point to the Spark
installation root directory. 

The target binaries are complied to the `bin/` directory. Add them to your path with 

```
echo "export PATH=[GNOCCHI INSTALLATION DIR]/gnocchi/bin:\$PATH" >> $HOME/.bashrc
source $HOME/.bashrc
```

You can then run `gnocchi` via `gnocchi-submit`, or open a shell using `gnocchi-shell`.

Test data is included. You can run with the test data by running:

```
gnocchi-submit regressPhenotypes examples/testData/5snps10samples.vcf examples/testData/10samples5Phenotypes2covars.txt ADDITIVE_LINEAR ../associations -saveAsText -phenoName pheno1 -covar -covarFile examples/testData/10samples5Phenotypes2covars.txt -covarNames pheno4,pheno5 -sampleIDName SampleID
```

## Phenotype Input

Format your phenotypes files as CSV or tab-delimited text, and include a header. 

```
SampleID    pheno1    pheno2
00001       0.001     0.002
```

Note: phenotypes and covariates must be numerical. For nominal scale data (i.e. categorical data), binarize. For ordinal scale data, convert to integers. 

# License

This project is released under an [Apache 2.0 license](LICENSE.txt).
