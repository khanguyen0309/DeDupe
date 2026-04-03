#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>

#include "hash_functions.h"

typedef struct {
    int index;
    unsigned char *hash;
} ChunkHash;

int compare_hashes(unsigned char *a, unsigned char *b, int n) {
    for (int i = 0; i < n; i++)
        if (a[i] != b[i])
            return 0;
    return 1;
}

// Function name: dedupe
// Description:   Computes a hash for each chunk of the input file, and the obtained hashes
//                to each other to determine the number of unique chunks in the file
void dedupe(char *filename, int chunk_size, char *output) {
	FILE *fp;
	char *buffer = (char *) malloc(chunk_size*sizeof(char));
	int hash_size = size_sha512(), n_hashes = 0;

	ChunkHash *chunks = NULL;
	int n_chunks = 0;
	
	// load chunks of the input file and hash them
	fp = fopen(filename, "r");
	assert(fp != NULL);

	
	while(fread(buffer, sizeof(char), chunk_size, fp) == chunk_size) {
        chunks = (ChunkHash *) realloc(chunks, (n_chunks + 1) * sizeof(ChunkHash));
        assert(chunks != NULL);

        chunks[n_chunks].index = n_chunks;
        chunks[n_chunks].hash = calculate_sha512((unsigned char *)buffer, chunk_size);

        n_chunks++;
	}
	fclose(fp);

	int mask[n_chunks];
	for(int i=0; i < n_chunks; i++)
		mask[i] = 0;
	for(int i=0; i < n_chunks; i++)
		for(int j=i+1; j < n_chunks; j++)
			if(compare_hashes(chunks[i].hash, chunks[j].hash, hash_size)) {	
				mask[j] = 1;
				break;
			}

	// print results
	fp = fopen(output, "w");
	assert(fp != NULL);
	for(int i=0; i < n_chunks; i++)
		fprintf(fp, "%d", mask[i]);
	fprintf(fp, "\n");
	fclose(fp);

	// release stuff
	free(buffer);
	for(int i=0; i < n_chunks; i++)
		free(chunks[i].hash);
	free(chunks);
}

