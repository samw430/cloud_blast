import sys
from multiprocessing import Queue
from threading import Thread

K_TUP_LEN = 6
MATCH_POINTS = 4
transition = -1
transversion = -2

def read_fasta(fasta_file):
	file_in = open(fasta_file, "r") 
	fasta_seq = []
	for line in file_in:
		if line[0] != ">":               # header lines start with '>'
			fasta_seq.append(line.strip())
	gene = ''.join(fasta_seq )
	return gene

def get_ktups(sequence, k):
	result = set()
	for i in range(0,len(sequence)-k+1):
		result.add(sequence[i:i+k])
	return result

def gap_rule(gap_len):
	return -(5 + gap_len*2)

def overlap_regions(search_string, subword_ktups, k):
	match_regions = []
	previous_region = None
	previous_region_score = None

	for i in range(0, len(search_string)-k+1):
		sub_search_string = search_string[i:i+k]
		match = False
		if sub_search_string in subword_ktups:
			match = True

		if match:
			if previous_region:
				previous_region_end = previous_region[1]
				gap = i - previous_region_end
				if gap > 1:
					if gap_rule(gap) + previous_region_score > 0:
						previous_region = (previous_region[0], i+k-1)
						previous_region_score = gap_rule(gap) + previous_region_score + MATCH_POINTS*k
					else:
						match_regions.append(previous_region)
						previous_region = (i, i+k-1)
						previous_region_score = MATCH_POINTS
				else:
					previous_region = (previous_region[0], i+k-1)
					previous_region_score = previous_region_score + MATCH_POINTS*(k+gap-1)
			else:
				previous_region = (i, i+k-1)
				previous_region_score = MATCH_POINTS	

	if previous_region:
		match_regions.append(previous_region)	

	return match_regions

def generate_global_memo(s, t):
	memo = [ [0 for i in range(len(s) + 1)] for j in range(len(t) + 1)]

	for i in range(len(s) + 1):
		memo[0][i] = -2 * i
	for j in range(len(t) + 1):
		memo[j][0] = -2 * j

	for i in range(1, len(t) + 1):
		for j in range(1, len(s) + 1):
			match = -1
			if t[i-1] == s[j-1]:
				match = 1
			memo[i][j] = max(memo[i-1][j] -2, memo[i][j-1] - 2, memo[i-1][j-1] + match)
	return memo

def global_traceback(memo, s, t):
	resultS = ""
	resultT = ""

	i = len(s)
	j = len(t)

	while i>=0 and j>=0:
		if i==0 and j==0:
			break
		elif i == 0:
			resultS = "-" + resultS
			resultT = t[j-1] + resultT
			j = j-1
		elif j == 0:
			resultT = "-" + resultT
			resultS = s[i-1] + resultS
			i = i-1
		else:
			if memo[i][j] == memo[i-1][j] - 2:
				resultT = '-' + resultT
				resultS = s[i-1] + resultS
				i = i-1
			elif (memo[i][j] == memo[i-1][j-1]-1 and s[i-1] != t[j-1]) or (memo[i][j] == memo[i-1][j-1]+1 and s[i-1]==t[j-1]):
				resultT = t[j-1] + resultT
				resultS = s[i-1] + resultS
				i = i-1
				j = j-1
			else:
				resultT = t[j-1] + resultT
				resultS = '-' + resultS
				j=j-1
	return (resultS, resultT)

def generate_k_band_memo(s, t, k):
	#s is along the rows and t is along the columns
	memo = [ [0 for i in range(len(t) + 1)] for j in range(len(s) + 1)]

	#Initialize row and column zero
	for i in range(k):
		memo[0][i] = -2 * i
	for j in range(k):
		memo[j][0] = -2 * j

	#Initialize diagonals with minimum integer
	for i in range(len(s) - k + 1):
		memo[i + k][i] = -sys.maxsize
	for j in range(len(t) - k + 1):
		memo[j][j+k] = -sys.maxsize

	t_len = len(t)

	#Fill in the k band
	for i in range(1, len(s) + 1):
		for j in range(1, 2*k):
			column = i - k + j
			if column>0 and column < t_len + 1:
				match = -1
				if s[i-1] == t[column-1]:
					match = 1
				memo[i][column] = max(memo[i-1][column] -2, memo[i][column-1] - 2, memo[i-1][column-1] + match)
	return memo

def print_2d(memo):
	for line in memo:
		print(line)

def alignment_score(resultS, resultT):
	score = 0
	for i in range(0,len(resultS)):
		if resultS[i] == resultT[i]:
			score += 1
		elif resultS[i] == "-":
			score -=2
		elif resultT[i] == "-":
			score -=2
		else:
			score -=1
	return score

def score_diagonal(search_string, subword_ktups, k):
	match_regions = []
	previous_region = None

	for i in range(0, len(search_string)-k+1):
		sub_search_string = search_string[i:i+k]
		match = False
		if sub_search_string in subword_ktups:
			match = True

		if match:
			if previous_region:
				previous_region_end = previous_region[1]
				gap = i - previous_region_end
				if gap > 1:
					match_regions.append(previous_region)
					previous_region = (i, i+k-1)
				else:
					previous_region = (previous_region[0], i+k-1)
			else:
				previous_region = (i, i+k-1)

	if previous_region:
		match_regions.append(previous_region)	

	score = 0
	for region in match_regions:
		score = score + region[1] - region[0]

	return score

def alus_in_subword(alu, human_chromosome_19, i, search_segment_end, result_queue):

	subword_ktups = get_ktups(alu, K_TUP_LEN)
	chromosome_length = len(human_chromosome_19)
	alu_length = len(alu)

	num_alus = 0
	while i < (chromosome_length - alu_length - 1) and i < search_segment_end:
		score = score_diagonal(human_chromosome_19[i: i + alu_length], subword_ktups, K_TUP_LEN)
		if score < 150:
			i = i + alu_length
		elif score > 175:
			memo = generate_k_band_memo(alu,human_chromosome_19[i: i+alu_length], 10)	
			aligned_strings = global_traceback(memo, alu, human_chromosome_19[i: i+alu_length])
			result_score = alignment_score(aligned_strings[0], aligned_strings[1])
			if result_score > 50:
				print(i, "of", chromosome_length, "got", num_alus)
				num_alus += 1
				i = i + alu_length
			i = i + 1
		else:
			i = i + 1
	result_queue.put(num_alus)

def main():
	mode = sys.argv[1]
	if mode not in ["serial", "parallel"]:
		print("Usage: python3 lab3.py [serial,parallel]")
		exit()

	human_chromosome_19 = read_fasta("human_chromosome_19.fasta")
	alu = "GGCCGGGCGCGGTGGCTCACGCCTGTAATCCCAGCACTTTGGGAGGCCGAGGCGGGCGGATCACCTGAGGTCAGGAGTTCGAGACCAGCCTGGCCAACATGGTGAAACCCCGTCTCTACTAAAAATACAAAAATTAGCCGGGCGTGGTGGCGCGCGCCTGTAATCCCAGCTACTCGGGAGGCTGAGGCAGGAGAATCGCTTGAACCCGGGAGGCGGAGGTTGCAGTGAGCCGAGATCGCGCCACTGCACTCCAGCCTGGGCGACAGAGCGAGACTCCGTCTCAAAAAAAA"

	if mode == "parallel":
		thread_list = list()
		result_queue = Queue()

		for i in range(0, len(human_chromosome_19), 5000000):
			thread = Thread(target=alus_in_subword, args=(alu, human_chromosome_19, i, i + 5000000, result_queue))
			thread_list.append(thread)
			thread.start()

		for thread in thread_list:
			thread.join()

		total_alus = 0
		while not result_queue.empty():
			total_alus += result_queue.get()

		print("Total Alus found ", total_alus)
	else:
		result_queue = Queue()
		alus_in_subword(alu, human_chromosome_19, 0, len(human_chromosome_19), result_queue)
		print("Total Alus found ", result_queue.get())

	#To do keep track of overlapping ALUs maybe?

if __name__ == '__main__':
	main()