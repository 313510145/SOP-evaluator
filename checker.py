import sys
import argparse
import random
from typing import List, Set, Tuple

def parse_spec_file(path: str) -> Tuple[int, Set[int], Set[int]]:
    with open(path, "r", encoding="utf-8") as f:
        lines = [ln.strip() for ln in f.readlines()]
    n_bit = int(lines[0])
    on_set = {int(tok) for tok in lines[1].split()} if lines[1] else set()
    dc_set = {int(tok) for tok in lines[2].split()} if lines[2] else set()
    return n_bit, on_set, dc_set

def read_sop_file(path: str, n_bit: int) -> List[str]:
    implicants = []
    with open(path, "r", encoding="utf-8") as f:
        for ln in f:
            line = ln.strip()
            if line == "":
                if n_bit == 0:
                    implicants.append("")
                continue
            implicants.append(line)
    return implicants

def validate_implicant(impl: str, n_bit: int) -> None:
    if n_bit == 0:
        if impl != "":
            raise ValueError("for n-bit=0, implicant must be an empty string.")
        return
    if len(impl) != n_bit:
        raise ValueError(f"implicant length error (expected {n_bit}): {impl!r}")
    for ch in impl:
        if ch not in {"0", "1", "-"}:
            raise ValueError(f"illegal character in implicant (only 0/1/- allowed): {impl!r}")

def iter_implicant_minterms(impl: str, n_bit: int):
    if n_bit == 0:
        yield 0
        return
    base_val = 0
    free_positions = []
    for i, ch in enumerate(impl):
        bitpos = n_bit - 1 - i
        if ch == "1":
            base_val |= (1 << bitpos)
        elif ch == "-":
            free_positions.append(bitpos)
    free_cnt = len(free_positions)
    for mask in range(1 << free_cnt):
        v = base_val
        m = mask
        j = 0
        while j < free_cnt:
            if m & 1:
                v |= (1 << free_positions[j])
            m >>= 1
            j += 1
        yield v

def reservoir_add(sample: list, k: int, total_seen: int, item):
    if k <= 0:
        return
    if total_seen <= k:
        sample.append(item)
    else:
        j = random.randint(1, total_seen)
        if j <= k:
            sample[j - 1] = item

def count_literals(implicants: List[str]) -> int:
    c = 0
    for impl in implicants:
        for ch in impl:
            if ch == "0" or ch == "1":
                c += 1
    return c

def main():
    parser = argparse.ArgumentParser(description="sum-of-products checker")
    parser.add_argument("spec_file", help="spec file (3 lines: n-bit, on-set, dc-set)")
    parser.add_argument("sop_file", help="SOP file (each line an implicant, MSB->LSB, only 0/1/-; for n-bit=0 use an empty line)")
    parser.add_argument("--quiet", action="store_true", help="only print PASS or FAIL (PASS prints literal count)")
    parser.add_argument("--summary", action="store_true", help="print summary stats and result only")
    parser.add_argument("--sample", type=int, default=0, help="number of example terms/pairs to list for errors (0 = do not list)")
    args = parser.parse_args()

    n_bit, on_set, dc_set = parse_spec_file(args.spec_file)
    union_allowed = on_set | dc_set

    implicants = read_sop_file(args.sop_file, n_bit)
    for impl in implicants:
        validate_implicant(impl, n_bit)

    lit_count = count_literals(implicants)
    literal_product_expect = len(on_set) * n_bit
    fail_literal_product = (n_bit > 0) and (len(on_set) > 0) and (lit_count >= literal_product_expect)

    seen = {}
    dup_count = 0
    dup_sample: List[str] = []
    k = max(0, args.sample)
    for idx, impl in enumerate(implicants):
        if impl in seen:
            dup_count += 1
            first = seen[impl]
            reservoir_add(dup_sample, k, dup_count, f"line {first} and line {idx}: {impl}")
        else:
            seen[impl] = idx

    covered_on: Set[int] = set()
    off_hit_count = 0
    off_hit_sample: List[int] = []
    valid_implicants = [impl.strip() for impl in implicants if impl.strip()]
    if valid_implicants:
        for impl in valid_implicants:
            for m in iter_implicant_minterms(impl, n_bit):
                if m in on_set:
                    covered_on.add(m)
                elif m not in union_allowed:
                    off_hit_count += 1
                    reservoir_add(off_hit_sample, k, off_hit_count, m)

    uncovered_count = 0
    uncovered_sample: List[int] = []
    if k > 0:
        for m in on_set:
            if m not in covered_on:
                uncovered_count += 1
                reservoir_add(uncovered_sample, k, uncovered_count, m)
    else:
        for m in on_set:
            if m not in covered_on:
                uncovered_count += 1

    passed = (dup_count == 0) and (uncovered_count == 0) and (off_hit_count == 0) and (not fail_literal_product)

    if args.quiet:
        if passed:
            print(f"PASS {lit_count}")
        else:
            print("FAIL")
        return

    if args.summary:
        print(f"bit width: {n_bit}")
        print(f"on-set size: {len(on_set)}")
        print(f"dc-set size: {len(dc_set)}")
        print(f"SOP implicants: {len(implicants)}")
        print(f"SOP literals: {lit_count}")
        print("result:", "PASS" if passed else "FAIL")
        return

    print("=" * 60)
    print(f"bit width: {n_bit}")
    print(f"on-set size: {len(on_set)}")
    print(f"dc-set size: {len(dc_set)}")
    print(f"SOP implicants: {len(implicants)}")
    print(f"SOP literals: {lit_count}")

    print("-" * 60)
    if dup_count:
        if k > 0 and dup_sample:
            print(f"[ERROR] duplicate implicants found ({dup_count}). examples: {dup_sample}")
        else:
            print(f"[ERROR] duplicate implicants found ({dup_count}).")
    else:
        print("[OK] no duplicate implicants.")

    print("-" * 60)
    if fail_literal_product:
        print(f"[ERROR] literal count >= |on-set| * n_bit ({lit_count} >= {len(on_set)} * {n_bit}).")
    else:
        print("[OK] literal count < |on-set| * n_bit.")
        if len(on_set) == 0:
            print("[NOTE] |on-set| = 0 (trivially satisfied).")
    
    print("-" * 60)
    if uncovered_count:
        if k > 0 and uncovered_sample:
            print(f"[ERROR] on-set not covered ({uncovered_count} terms). examples: {uncovered_sample}")
        else:
            print(f"[ERROR] on-set not covered ({uncovered_count} terms).")
    else:
        print("[OK] all on-set terms covered.")

    print("-" * 60)
    if off_hit_count:
        if k > 0 and off_hit_sample:
            print(f"[ERROR] SOP covers off-set ({off_hit_count} terms). examples: {off_hit_sample}")
        else:
            print(f"[ERROR] SOP covers off-set ({off_hit_count} terms).")
    else:
        print("[OK] SOP does not cover off-set.")

    print("=" * 60)
    print("[RESULT]", "PASS" if passed else "FAIL")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[EXCEPTION] {e}", file=sys.stderr)
        sys.exit(1)
