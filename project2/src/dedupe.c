#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>

#include "hash_functions.h"
#include <pthread.h>
#include <stdbool.h>

#define INITIAL_CAPACITY 1024
#define MAX_PARALLEL_WORKERS 8
#define STDIO_OUTPUT_BUFSZ ((size_t)1 << 20)

// Chunk structure
typedef struct {
    int index;
    unsigned char *hash;
} ChunkHash;


// Shared Memory structure
typedef struct {
	ChunkHash *data;
	int count;
	int capacity;
	pthread_mutex_t lock;
} SharedMem;

// Thread argument structure
typedef struct {
    ChunkHash *input_array;
    int start;
    int end;
    SharedMem *dest_mem;
    SharedMem *src_mem;
} ThreadArgs;

typedef struct {
	ChunkHash *chunks;
	SharedMem *final_mem;
	char *out_buf;
	int start;
	int end;
	int hash_size;
} ClassifyArgs;

static void *classify_worker(void *arg) {
	ClassifyArgs *t = (ClassifyArgs *)arg;
	const int hs = t->hash_size;

	for (int i = t->start; i < t->end; i++) {
		int j = 0;
		for (; j < t->final_mem->count; j++) {
			if (t->final_mem->data[j].hash != NULL &&
			    memcmp(t->final_mem->data[j].hash, t->chunks[i].hash,
				   (size_t)hs) == 0) {
				break;
			}
		}
		assert(j < t->final_mem->count);
		t->out_buf[i] =
		    (t->final_mem->data[j].index == i) ? '0' : '1';
	}
	return NULL;
}

// Helper to initialize a SharedMem
static SharedMem* create_mem(int cap) {
    if (cap < 1) cap = 1;
    SharedMem *m = (SharedMem *)malloc(sizeof(SharedMem));
    assert(m != NULL);
    m->data = (ChunkHash *)malloc(sizeof(ChunkHash) * (size_t)cap);
    assert(m->data != NULL);
    m->count = 0;
    m->capacity = cap;
    pthread_mutex_init(&m->lock, NULL);
    return m;
}

// Helper to free a SharedMem and its contents
static void destroy_mem(SharedMem *m) {
    if (!m) return;
    pthread_mutex_destroy(&m->lock);
    free(m->data);
    free(m);
}

// Helper to add a unique chunk to SharedMem
static void add_chunk(SharedMem *shared, ChunkHash chunk) {
	pthread_mutex_lock(&shared->lock);

	bool found = false;
	const int hash_size = size_sha512();
	for (int i = 0; i < shared->count; i++) {
		if (shared->data[i].hash != NULL && chunk.hash != NULL &&
		    memcmp(shared->data[i].hash, chunk.hash, (size_t)hash_size) == 0) {
			found = true;

			if (shared->data[i].index > chunk.index) {
				shared->data[i].index = chunk.index;
			}
			break;
		}
	}

	if (!found) {
		if (shared->count >= shared->capacity) {
			shared->capacity *= 2;
			ChunkHash *tmp = (ChunkHash *)realloc(shared->data,
				(size_t)shared->capacity * sizeof(ChunkHash));
			assert(tmp != NULL);
			shared->data = tmp;
		}
		shared->data[shared->count++] = chunk;
	}

	pthread_mutex_unlock(&shared->lock);
}


// Worker function for threads 
static void *worker(void *arg) {
	ThreadArgs *t = (ThreadArgs *)arg;

	if (t->input_array != NULL) {
        for (int i = t->start; i < t->end; i++) {
            add_chunk(t->dest_mem, t->input_array[i]);
        }
    } else if (t->src_mem != NULL) {
        for (int i = 0; i < t->src_mem->count; i++) {
            add_chunk(t->dest_mem, t->src_mem->data[i]);
        }
    }

	return NULL;
}

// Function name: dedupe
// Description:   Computes a hash for each chunk of the input file, and the obtained hashes
//                to each other to determine the number of unique chunks in the file
void dedupe(char *filename, int chunk_size, char *output) {
	FILE *fp;
	char *buffer = (char *)malloc((size_t)chunk_size * sizeof(char));
	assert(buffer != NULL);
	int hash_size = size_sha512();

	ChunkHash *chunks = NULL;
	int n_chunks = 0;

	// load chunks of the input file and hash them
	fp = fopen(filename, "r");
	assert(fp != NULL);

	while (fread(buffer, sizeof(char), (size_t)chunk_size, fp) == (size_t)chunk_size) {
		ChunkHash *tmp = (ChunkHash *)realloc(chunks, (size_t)(n_chunks + 1) * sizeof(ChunkHash));
		assert(tmp != NULL);
		chunks = tmp;

		chunks[n_chunks].index = n_chunks;
		chunks[n_chunks].hash = calculate_sha512((unsigned char *)buffer, chunk_size);
		assert(chunks[n_chunks].hash != NULL);
		n_chunks++;
	}
	fclose(fp);

	// If the file is smaller than one chunk, match baseline: one newline only.
	if (n_chunks == 0) {
		fp = fopen(output, "wb");
		assert(fp != NULL);
		fputc('\n', fp);
		fclose(fp);
		free(buffer);
		free(chunks);
		return;
	}

	// Use up to MAX_PARALLEL_WORKERS threads, but never more than n_chunks, and avoid 0-sized ranges.
	int n_threads0 = MAX_PARALLEL_WORKERS;
	if (n_threads0 > n_chunks) n_threads0 = n_chunks;
	if (n_threads0 < 1) n_threads0 = 1;
	int n_mems0 = (n_threads0 + 1) / 2;

	// Level 0: N threads into ceil(N/2) mems
	SharedMem **lv0_mems = (SharedMem **)malloc((size_t)n_mems0 * sizeof(SharedMem *));
	assert(lv0_mems != NULL);
	for (int i = 0; i < n_mems0; i++) lv0_mems[i] = create_mem(INITIAL_CAPACITY);

	pthread_t *threads = (pthread_t *)malloc((size_t)n_threads0 * sizeof(pthread_t));
	ThreadArgs *thread_args = (ThreadArgs *)calloc((size_t)n_threads0, sizeof(ThreadArgs));
	assert(threads != NULL && thread_args != NULL);

	int per = (n_chunks + n_threads0 - 1) / n_threads0; // ceil
	for (int i = 0; i < n_threads0; i++) {
		int start = i * per;
		int end = start + per;
		if (end > n_chunks) end = n_chunks;

		thread_args[i].input_array = chunks;
		thread_args[i].start = start;
		thread_args[i].end = end;
		thread_args[i].dest_mem = lv0_mems[i / 2];
		thread_args[i].src_mem = NULL;
		pthread_create(&threads[i], NULL, worker, (void *)&thread_args[i]);
	}
	for (int i = 0; i < n_threads0; i++) pthread_join(threads[i], NULL);

	free(threads);
	free(thread_args);

	// Level 1: merge lv0 mems into 2 mems (or 1 if only one input mem)
	int n_mems1 = (n_mems0 > 1) ? 2 : 1;
	SharedMem *lv1_mems[2] = {0};
	for (int i = 0; i < n_mems1; i++) lv1_mems[i] = create_mem(n_chunks);

	pthread_t lv1_threads[4];
	ThreadArgs lv1_thread_args[4] = {0};
	for (int i = 0; i < n_mems0; i++) {
		lv1_thread_args[i].input_array = NULL;
		lv1_thread_args[i].src_mem = lv0_mems[i];
		lv1_thread_args[i].dest_mem = lv1_mems[(n_mems1 == 1) ? 0 : (i / 2)];
		pthread_create(&lv1_threads[i], NULL, worker, (void *)&lv1_thread_args[i]);
	}
	for (int i = 0; i < n_mems0; i++) pthread_join(lv1_threads[i], NULL);

	for (int i = 0; i < n_mems0; i++) destroy_mem(lv0_mems[i]);
	free(lv0_mems);

	// Level 2: merge lv1 mems into final
	SharedMem *final_mem = create_mem(n_chunks);
	pthread_t lv2_threads[2];
	ThreadArgs lv2_thread_args[2] = {0};
	for (int i = 0; i < n_mems1; i++) {
		lv2_thread_args[i].input_array = NULL;
		lv2_thread_args[i].src_mem = lv1_mems[i];
		lv2_thread_args[i].dest_mem = final_mem;
		pthread_create(&lv2_threads[i], NULL, worker, (void *)&lv2_thread_args[i]);
	}
	for (int i = 0; i < n_mems1; i++) pthread_join(lv2_threads[i], NULL);

	for (int i = 0; i < n_mems1; i++) destroy_mem(lv1_mems[i]);

	char *out_buf = (char *)malloc((size_t)n_chunks);
	assert(out_buf != NULL);

	int n_classify = MAX_PARALLEL_WORKERS;
	if (n_classify > n_chunks) n_classify = n_chunks;
	if (n_classify < 1) n_classify = 1;

	pthread_t *classify_threads =
	    (pthread_t *)malloc((size_t)n_classify * sizeof(pthread_t));
	ClassifyArgs *classify_args = (ClassifyArgs *)calloc(
	    (size_t)n_classify, sizeof(ClassifyArgs));
	assert(classify_threads != NULL && classify_args != NULL);

	int c_per = (n_chunks + n_classify - 1) / n_classify;
	for (int i = 0; i < n_classify; i++) {
		int c_start = i * c_per;
		int c_end = c_start + c_per;
		if (c_end > n_chunks) c_end = n_chunks;

		classify_args[i].chunks = chunks;
		classify_args[i].final_mem = final_mem;
		classify_args[i].out_buf = out_buf;
		classify_args[i].start = c_start;
		classify_args[i].end = c_end;
		classify_args[i].hash_size = hash_size;
		pthread_create(&classify_threads[i], NULL, classify_worker,
			       (void *)&classify_args[i]);
	}
	for (int i = 0; i < n_classify; i++)
		pthread_join(classify_threads[i], NULL);

	free(classify_threads);
	free(classify_args);

	fp = fopen(output, "wb");
	assert(fp != NULL);
	if ((size_t)n_chunks >= (size_t)BUFSIZ) {
		size_t io_sz = (size_t)n_chunks;
		if (io_sz > STDIO_OUTPUT_BUFSZ) io_sz = STDIO_OUTPUT_BUFSZ;
		setvbuf(fp, NULL, _IOFBF, io_sz);
	}
	size_t written = fwrite(out_buf, 1, (size_t)n_chunks, fp);
	assert(written == (size_t)n_chunks);
	fputc('\n', fp);
	fclose(fp);
	free(out_buf);

	// Cleanup
	for (int i = 0; i < n_chunks; i++) free(chunks[i].hash);
	free(chunks);

	destroy_mem(final_mem);
}

