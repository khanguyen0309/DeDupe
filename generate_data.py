import os
import random
import string

TEST_DIR = "tests"
CONFIG_DIR = "config"
ENGLISH_CHARS = string.ascii_letters

def main():
    os.makedirs(TEST_DIR, exist_ok=True)
    os.makedirs(CONFIG_DIR, exist_ok=True)

    print("Starting exact specification test generation...\n")

    for i in range(1, 21):
        # ----------------------------------------------------
        # TESTS 1-10: Reserved for manual generation
        # ----------------------------------------------------
        if 1 <= i <= 10:
            print(f"Skipping Test {i:<2} (Reserved for manual creation)")
            continue

        size = 0
        chunks = 0
        duplicates = 0
        is_single_char_chunk = False

        # ----------------------------------------------------
        # TESTS 11-15: 1-10 kB, 100 chunks, 5-20 duplicates
        # ----------------------------------------------------
        if 11 <= i <= 15:
            size = random.randint(1024, 10240)  # 1 kB to 10 kB
            chunks = 100
            duplicates = random.randint(5, 20)

        # ----------------------------------------------------
        # TESTS 16-17: 20-50 kB, 500 chunks, 5-20 duplicates
        # ----------------------------------------------------
        elif 16 <= i <= 17:
            size = random.randint(20480, 51200) # 20 kB to 50 kB
            chunks = 500
            duplicates = random.randint(5, 20)

        # ----------------------------------------------------
        # TEST 18: 20-50 kB, chunk size = 1 character
        # ----------------------------------------------------
        elif i == 18:
            size = random.randint(20480, 51200)
            chunks = size # Chunk size of 1 means chunk count = file size
            is_single_char_chunk = True

        # ----------------------------------------------------
        # TEST 19: 100 kB, 10 chunks
        # ----------------------------------------------------
        elif i == 19:
            size = 102400 # 100 kB
            chunks = 10
            duplicates = 0 # No specific duplicates requested, kept unique

        # ----------------------------------------------------
        # TEST 20: 100 kB, chunk size = 1 character
        # ----------------------------------------------------
        elif i == 20:
            size = 102400
            chunks = size # Chunk size of 1
            is_single_char_chunk = True

        # ====================================================
        # GENERATION LOGIC
        # ====================================================
        test_content = ""

        if is_single_char_chunk:
            # If chunk size is 1, we just write pure characters.
            # (With only 52 letters, duplicates happen natively and massively)
            test_content = "".join(random.choices(ENGLISH_CHARS, k=size))
        else:
            chunk_size = size // chunks
            unique_count = chunks - duplicates
            
            # 1. Create a set of guaranteed unique chunks
            pool = set()
            while len(pool) < unique_count:
                pool.add("".join(random.choices(ENGLISH_CHARS, k=chunk_size)))
            pool = list(pool)

            # 2. Re-add randomly selected duplicates to hit the total chunk count
            seq = pool.copy()
            seq.extend(random.choices(pool, k=duplicates))
            
            # 3. Shuffle so the duplicates are naturally dispersed
            random.shuffle(seq)
            test_content = "".join(seq)

            # 4. Pad any remaining bytes at the end of the file 
            # (Happens if the file size doesn't divide perfectly by chunk count)
            remaining_bytes = size - len(test_content)
            if remaining_bytes > 0:
                test_content += "".join(random.choices(ENGLISH_CHARS, k=remaining_bytes))

        # ====================================================
        # FILE WRITING
        # ====================================================
        with open(os.path.join(TEST_DIR, f"test{i}.txt"), "w", encoding="ascii") as f:
            f.write(test_content)
            
        with open(os.path.join(CONFIG_DIR, f"config{i}.txt"), "w", encoding="ascii") as f:
            f.write(str(chunks))
            
        print(f"Generated Test {i:<2}: {size/1024:>6.1f} kB | {chunks:>6} chunks | {duplicates:>2} explicit duplicates")

    print("\nSuccess! Files 11 through 20 have been populated.")

if __name__ == "__main__":
    main()