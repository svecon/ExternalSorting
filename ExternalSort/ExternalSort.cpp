#include <iostream>
#include <vector>
#include <string>
#include <fstream>
#include <cstdio>

using namespace std;

//#include <chrono>
//#include <ctime>
//void printtime() {
//	std::time_t end_time = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
//	std::cout << std::ctime(&end_time) << endl;
//}

struct KVpair {
	unsigned long long k;
	unsigned long long v;

	KVpair() {}

	KVpair(unsigned long long k, unsigned long long v) {
		this->k = k;
		this->v = v;
	}

	bool operator < (const KVpair& str) const { 
		if (k == str.k) {
			return v < str.v;
		}
		return (k < str.k);
	}
};
typedef vector<KVpair> VS;
typedef VS* VSP;

struct SBFile {
	size_t count;
	int order;
	VSP cache;
	ifstream * fin;

	size_t cur = 0;
	streamsize max = 0;
	bool isLast = false;

	SBFile(size_t c, int n) {
		count = c;
		order = n;
	}

	SBFile(size_t c, int n, VSP largebuffer) {
		count = c;
		order = n;
		cache = largebuffer;
		max = c;
		isLast = true;
	}

	void close() {
		if (isLast) { return; }

		fin->close();
		delete cache;
	}

	void openAndRead() {
		if (isLast) { return; }

		fin = new ifstream("data." + to_string(order) + ".bin", ios::in | ios::binary);
		cache = new VS(64 * 1024);
		readNext();
	}

	void readNext() {
		fin->read((char*)(&(*cache)[0]), cache->size()*sizeof(KVpair));
		cur = 0;
		max = fin->gcount() / sizeof(KVpair);
	}

	KVpair read() {
		return (*cache)[cur];
	}

	bool pop() {
		cur++;

		if (cur == max) {
			if (isLast) {
				return false;
			}

			readNext();
			cur = 0;

			if (max == 0) {
				return false;
			}
		}

		return true;
	}
};
typedef vector<SBFile> VBF;
typedef VBF* VBFP;

void insertsort(VSP arr, size_t l, size_t r) {
	KVpair temp;
	for (size_t i = l + 1; i <= r; ++i) {
		size_t x = i;
		temp = (*arr)[x];

		while (x > 0 && temp < (*arr)[x - 1]) {
			(*arr)[x] = (*arr)[x-1];
			--x;
		}

		(*arr)[x] = temp;
	}
}
void quicksort(VSP arr, size_t l, size_t r) {
	if (l >= r) { return; }

	if (r - l <= 4) {
		insertsort(arr, l, r);
		return;
	}

	KVpair temp, p;
	size_t i, j;
	i = l;
	j = r;
	p = (*arr)[(r + l) / 2];

	// median of 3
	if ((*arr)[l] < p && (*arr)[l] < p) {
		if ((*arr)[l] < (*arr)[r]) {
			p = (*arr)[r];
		}
		else {
			p = (*arr)[l];
		}
	}
	else if (p < (*arr)[l] && p < (*arr)[l]) {
		if ((*arr)[l] < (*arr)[r]) {
			p = (*arr)[l];
		}
		else {
			p = (*arr)[r];
		}
	}


	while (i <= j) {
		while ((*arr)[i] < p) { ++i; }
		while (p < (*arr)[j]) { --j; }

		if (i < j) {
			temp = (*arr)[i];
			(*arr)[i] = (*arr)[j];
			(*arr)[j] = temp;
		}

		if (i <= j) {
			++i;
			--j;
		}
	}

	quicksort(arr, l, j);
	quicksort(arr, i, r);
}

void sortAndWriteTempFile(VSP largeBuffer, size_t count, int c) {
	//cout << "sorting block" << endl; printtime();
	quicksort(largeBuffer, 0, count - 1);
	//cout << "writing binary block" << endl; printtime();
	
	ofstream fout("data." + to_string(c) + ".bin", ios::out | ios::binary);
	fout.write((char*)&(*largeBuffer)[0], count * sizeof(KVpair));
	fout.close();	
	
	//cout << "finished writing binary block" << endl; printtime();
}
VBFP parseTextFile(VSP arr, ifstream & myfile) {

	//cout << "reading huge file" << endl; printtime();

	const int BUFFER_SIZE = 64 * 1024;
	char buffer[BUFFER_SIZE];

	VBFP sortedFiles = new VBF();

	int tempFile = 0;
	unsigned long long k, num = 0;
	size_t i, cur = 0;
	streamsize noBytesRead;
	while (!myfile.eof()) {
		myfile.read(&buffer[0], BUFFER_SIZE);
		noBytesRead = myfile.gcount();

		i = 0;
		while (i < noBytesRead) {
			if (buffer[i] == ' ') {
				k = num; num = 0;
				++i;
			}
			else if (buffer[i] == '\n') {
				(*arr)[cur++] = KVpair(k, num);
				num = 0; ++i;

				if (cur == arr->size()) {
					sortAndWriteTempFile(arr, cur, tempFile);
					sortedFiles->push_back(SBFile(cur, tempFile));
					++tempFile;
					cur = 0;
				}
			}
			else {
				num *= 10;
				num += buffer[i] - '0';
				++i;
			}
		}
	}

	// if there is not a newline at the end
	if (num > 0 && k > 0) { (*arr)[cur++] = KVpair(k, num); }

	if (cur > 0) {
		//cout << "sorting LAST block" << endl; printtime();
		quicksort(arr, 0, cur - 1);
		sortedFiles->push_back(SBFile(cur, tempFile, arr));
	}

	//cout << "finished reading huge file" << endl; printtime();

	return sortedFiles;
}

int hugesort(string fileName, size_t maxRecords) {

	VSP largeBuffer = new VS(maxRecords);
	ifstream hugeTextFile(fileName, ios_base::in);
	if (!hugeTextFile.is_open()) {
		cerr << "There was an error while opening the file: " << fileName << endl;
		return 1;
	}

	//cout << "reading ... " << fileName << endl; printtime();

	VBFP sortedFiles = parseTextFile(largeBuffer, hugeTextFile);
	
	//cout << "finished reading ... " << fileName << endl;
	
	hugeTextFile.close();

	//cout << "opening binary files" << endl; printtime();

	for (auto i = sortedFiles->begin(); i != sortedFiles->end(); ++i) {
		i->openAndRead();
	}

	KVpair min;
	vector<int> toErase;
	//FILE *fp = stdout;
	FILE *fp = fopen("data.out", "w");

	//cout << "writing complete file" << endl; printtime();

	size_t noFiles = sortedFiles->size();

	while (noFiles > 0) {
	
		min = (*sortedFiles)[0].read();

		for (size_t i = 0 + 1; i < sortedFiles->size(); ++i) {
			if ((*sortedFiles)[i].read() < min) {
				min = (*sortedFiles)[i].read();
			}
		}

		for (auto i = sortedFiles->begin(); i != sortedFiles->end(); ++i) {
			while (i->read().k == min.k) {
				if (i->pop() == false) {
					toErase.push_back(i - sortedFiles->begin());
					//cout << "finished reading " << i->order << ". file" << endl; printtime();
					break;
				}
			}
		}

		for (size_t i = toErase.size(); i--> 0; ) {
			(*sortedFiles)[toErase[i]].close();
			remove(("data." + to_string((*sortedFiles)[toErase[i]].order) + ".bin").c_str());
			sortedFiles->erase(sortedFiles->begin() + toErase[i]);
			--noFiles;
		}
		toErase.clear();

		fprintf(fp, "%llu %llu\n", min.k, min.v);
	}
		
	delete largeBuffer;
	delete sortedFiles;
	fclose(fp);
	//cout << "closed compplete file" << endl; printtime();

	return 0;
}

int main(int argc, char * arv[]) {
	string filename = "data.txt";
	size_t maxRecords = 300 * 1000 * 1000;

	if (argc >= 2) {
		filename = arv[1];
	}

	if (argc >= 3) {
		maxRecords = stoi(arv[2]);
	}

	return hugesort(filename, maxRecords);

	//cout << "FINISHED!" << endl;
}


