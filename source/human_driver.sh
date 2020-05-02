set -x
rm -rf output
rm -rf temp_seeds

if make local; then
	~/hadoop-2.9.2/bin/hadoop jar build.jar Blast ../query_strings/alu.txt ../reference_genomes/10_percent_human.fasta output
fi

