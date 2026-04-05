#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include <stdint.h>
#include <stdatomic.h>

#include "hash_functions.h"
#include <pthread.h>

#define MAX_PARALLEL_WORKERS 8
#define STDIO_OUTPUT_BUFSZ ((size_t)1 << 20)
#define NUM_STRIPES 1024
#define HT_SLOT_FACTOR 2
#define HT_MIN_SLOTS_LEN 16

typedef struct {
	int index;
	unsigned char *hash;
} ChunkHash;

typedef struct {
	ChunkHash *data;
	int *slots;
	int slots_len;
	int n_chunks;
	_Atomic int next_data;
	pthread_mutex_t stripes[NUM_STRIPES];
} GlobalTable;

typedef struct {
	unsigned char *file_buf;
	int chunk_size;
	ChunkHash *chunks;
	int start;
	int end;
	GlobalTable *gt;
} HashInsertArgs;

typedef struct {
	ChunkHash *chunks;
	GlobalTable *gt;
	char *out_buf;
	int start;
	int end;
	int hash_size;
} ClassifyArgs;

/* Map a digest to a starting slot; table length is a power of two. */
static int key_slot(const unsigned char *digest, int slots_len)
{
	uint64_t prefix;
	memcpy(&prefix, digest, 8);
	return (int)(prefix & (uint64_t)(slots_len - 1));
}

static int round_up_pow2(int x)
{
	if (x <= 1)
		return 1;
	unsigned u = (unsigned)x - 1u;
	u |= u >> 1;
	u |= u >> 2;
	u |= u >> 4;
	u |= u >> 8;
	u |= u >> 16;
	return (int)(u + 1u);
}

static void global_table_init(GlobalTable *t, int n_chunks)
{
	int need = HT_SLOT_FACTOR * n_chunks;
	if (need < HT_MIN_SLOTS_LEN)
		need = HT_MIN_SLOTS_LEN;
	t->slots_len = round_up_pow2(need);
	t->n_chunks = n_chunks;
	atomic_init(&t->next_data, 0);

	t->data = (ChunkHash *)calloc((size_t)n_chunks, sizeof(ChunkHash));
	assert(t->data != NULL);
	t->slots = (int *)malloc((size_t)t->slots_len * sizeof(int));
	assert(t->slots != NULL);
	for (int i = 0; i < t->slots_len; i++)
		t->slots[i] = -1;

	for (int s = 0; s < NUM_STRIPES; s++)
		pthread_mutex_init(&t->stripes[s], NULL);
}

static void global_table_destroy(GlobalTable *t)
{
	for (int s = 0; s < NUM_STRIPES; s++)
		pthread_mutex_destroy(&t->stripes[s]);
	free(t->slots);
	free(t->data);
}

static void striped_add_chunk(GlobalTable *t, const ChunkHash *chunk)
{
	const int hash_size = size_sha512();
	int idx = key_slot(chunk->hash, t->slots_len);

	for (;;) {
		int stripe = idx % NUM_STRIPES;
		pthread_mutex_lock(&t->stripes[stripe]);

		int si = t->slots[idx];
		if (si < 0) {
			int di = atomic_fetch_add_explicit(&t->next_data, 1,
							     memory_order_acq_rel);
			assert(di < t->n_chunks);
			t->data[di] = *chunk;
			t->slots[idx] = di;
			pthread_mutex_unlock(&t->stripes[stripe]);
			return;
		}
		if (memcmp(t->data[si].hash, chunk->hash, (size_t)hash_size) ==
		    0) {
			if (t->data[si].index > chunk->index)
				t->data[si].index = chunk->index;
			pthread_mutex_unlock(&t->stripes[stripe]);
			return;
		}
		pthread_mutex_unlock(&t->stripes[stripe]);
		idx = (idx + 1) & (t->slots_len - 1);
	}
}

static int lookup_canonical_index(GlobalTable *t, const unsigned char *hash,
				  int hs)
{
	int idx = key_slot(hash, t->slots_len);
	for (;;) {
		int si = t->slots[idx];
		assert(si >= 0);
		if (memcmp(t->data[si].hash, hash, (size_t)hs) == 0)
			return t->data[si].index;
		idx = (idx + 1) & (t->slots_len - 1);
	}
}

static void *hash_insert_worker(void *arg)
{
	HashInsertArgs *t = (HashInsertArgs *)arg;
	const size_t cs = (size_t)t->chunk_size;

	for (int i = t->start; i < t->end; i++) {
		t->chunks[i].index = i;
		t->chunks[i].hash = calculate_sha512(
		    t->file_buf + (size_t)i * cs, t->chunk_size);
		assert(t->chunks[i].hash != NULL);
		striped_add_chunk(t->gt, &t->chunks[i]);
	}
	return NULL;
}

static void *classify_worker(void *arg)
{
	ClassifyArgs *t = (ClassifyArgs *)arg;
	const int hs = t->hash_size;

	for (int i = t->start; i < t->end; i++) {
		int canon = lookup_canonical_index(t->gt, t->chunks[i].hash,
						   hs);
		t->out_buf[i] = (canon == i) ? '0' : '1';
	}
	return NULL;
}

void dedupe(char *filename, int chunk_size, char *output)
{
	FILE *fp;
	int hash_size = size_sha512();

	fp = fopen(filename, "r");
	assert(fp != NULL);
	assert(fseek(fp, 0, SEEK_END) == 0);
	long file_len = ftell(fp);
	assert(file_len >= 0);
	assert(fseek(fp, 0, SEEK_SET) == 0);

	int n_chunks = 0;
	if (chunk_size > 0) {
		n_chunks = (int)((unsigned long long)file_len /
				 (unsigned long long)chunk_size);
	}

	if (n_chunks <= 0) {
		fclose(fp);
		fp = fopen(output, "wb");
		assert(fp != NULL);
		fputc('\n', fp);
		fclose(fp);
		return;
	}

	size_t payload = (size_t)n_chunks * (size_t)chunk_size;
	unsigned char *file_buf =
	    (unsigned char *)malloc(payload * sizeof(unsigned char));
	assert(file_buf != NULL);
	size_t got = fread(file_buf, 1, payload, fp);
	fclose(fp);
	assert(got == payload);

	ChunkHash *chunks =
	    (ChunkHash *)malloc((size_t)n_chunks * sizeof(ChunkHash));
	assert(chunks != NULL);

	GlobalTable *gt = (GlobalTable *)malloc(sizeof(GlobalTable));
	assert(gt != NULL);
	global_table_init(gt, n_chunks);

	int n_workers = MAX_PARALLEL_WORKERS;
	if (n_workers > n_chunks)
		n_workers = n_chunks;
	if (n_workers < 1)
		n_workers = 1;

	pthread_t *workers =
	    (pthread_t *)malloc((size_t)n_workers * sizeof(pthread_t));
	HashInsertArgs *work_args =
	    (HashInsertArgs *)calloc((size_t)n_workers, sizeof(HashInsertArgs));
	assert(workers != NULL && work_args != NULL);

	int w_per = (n_chunks + n_workers - 1) / n_workers;
	for (int i = 0; i < n_workers; i++) {
		int w_start = i * w_per;
		int w_end = w_start + w_per;
		if (w_end > n_chunks)
			w_end = n_chunks;
		work_args[i].file_buf = file_buf;
		work_args[i].chunk_size = chunk_size;
		work_args[i].chunks = chunks;
		work_args[i].start = w_start;
		work_args[i].end = w_end;
		work_args[i].gt = gt;
		pthread_create(&workers[i], NULL, hash_insert_worker,
			       (void *)&work_args[i]);
	}
	for (int i = 0; i < n_workers; i++)
		pthread_join(workers[i], NULL);

	free(workers);
	free(work_args);
	free(file_buf);

	char *out_buf = (char *)malloc((size_t)n_chunks);
	assert(out_buf != NULL);

	int n_classify = MAX_PARALLEL_WORKERS;
	if (n_classify > n_chunks)
		n_classify = n_chunks;
	if (n_classify < 1)
		n_classify = 1;

	pthread_t *classify_threads =
	    (pthread_t *)malloc((size_t)n_classify * sizeof(pthread_t));
	ClassifyArgs *classify_args = (ClassifyArgs *)calloc(
	    (size_t)n_classify, sizeof(ClassifyArgs));
	assert(classify_threads != NULL && classify_args != NULL);

	int c_per = (n_chunks + n_classify - 1) / n_classify;
	for (int i = 0; i < n_classify; i++) {
		int c_start = i * c_per;
		int c_end = c_start + c_per;
		if (c_end > n_chunks)
			c_end = n_chunks;

		classify_args[i].chunks = chunks;
		classify_args[i].gt = gt;
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
		if (io_sz > STDIO_OUTPUT_BUFSZ)
			io_sz = STDIO_OUTPUT_BUFSZ;
		setvbuf(fp, NULL, _IOFBF, io_sz);
	}
	size_t written = fwrite(out_buf, 1, (size_t)n_chunks, fp);
	assert(written == (size_t)n_chunks);
	fputc('\n', fp);
	fclose(fp);
	free(out_buf);

	for (int i = 0; i < n_chunks; i++)
		free(chunks[i].hash);
	free(chunks);

	global_table_destroy(gt);
	free(gt);
}
