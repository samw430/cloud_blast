set -x
rm -rf output
rm -rf temp_seeds

if make local; then
	~/hadoop-2.9.2/bin/hadoop jar build.jar Blast ../query_strings/ecoli_query.txt ../reference_genomes/ecoliK12.fasta output
fi

