import pickle


def get_data(infile):
	return pickle.load(open( infile, "rb"))
if __name__ == '__main__':
	main()