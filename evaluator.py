import argparse
import csv
import re
import shutil
import subprocess
import tarfile
import tempfile
import time
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
import sys

def run_cmd(cmd: List[str], cwd: Path, timeout_sec: Optional[int] = None) -> subprocess.CompletedProcess:
    try:
        return subprocess.run(cmd, cwd=str(cwd), stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout_sec, check=False, text=True, errors="replace")
    except subprocess.TimeoutExpired:
        return subprocess.CompletedProcess(cmd, returncode=124, stdout="", stderr="[TIMEOUT]")

def time_run_exec(exec_cmd: List[str], cwd: Path, timeout_sec: int) -> Tuple[float, subprocess.CompletedProcess]:
    t0 = time.monotonic()
    cp = run_cmd(exec_cmd, cwd, timeout_sec)
    t1 = time.monotonic()
    return t1 - t0, cp

def discover_submissions(root: Path) -> List[Path]:
    subs = [p for p in root.iterdir() if p.is_dir()]
    subs.sort()
    return subs

def discover_cases(cases_dir: Path) -> List[Path]:
    files = [p for p in cases_dir.iterdir() if p.is_file()]
    files.sort()
    return files

def extract_tar_in_dir(sub_dir: Path) -> Optional[Path]:
    tars = list(sub_dir.glob("*.tar"))
    if not tars:
        return None
    tar_path = tars[0]
    with tarfile.open(tar_path, "r") as tar:
        tar.extractall(path=sub_dir)
    preferred = sub_dir / tar_path.stem
    if preferred.exists() and preferred.is_dir():
        return preferred
    dirs = [d for d in sub_dir.iterdir() if d.is_dir()]
    dirs.sort()
    return dirs[0] if dirs else None

def get_student_id(name: str) -> str:
    sid = name.split()[0]
    if sid.startswith("\\"):
        sid = sid[1:]
    return sid

def rank_and_score(case_results: List[Dict[str, Any]]) -> None:
    passes = [r for r in case_results if r["pass"] == "PASS"]
    passes.sort(key=lambda r: (r["lit"], r["runtime"]))
    N = len(passes)
    for idx, r in enumerate(passes, start=1):
        r["rank"] = idx
        base = 15
        bonus = (N - idx + 1) / N * 5 if N > 0 else 0
        r["score"] = round(base + bonus, 2)
    for r in case_results:
        if r["pass"] != "PASS":
            r["rank"] = ""
            r["score"] = 0

def parse_checker_quiet_line(s: str) -> Tuple[bool, Optional[int]]:
    lines = [ln.strip() for ln in (s or "").strip().splitlines() if ln.strip()]
    last = lines[-1] if lines else ""
    if last.startswith("PASS"):
        m = re.match(r"^PASS\s+(\d+)\s*$", last)
        return True, int(m.group(1)) if m else None
    return False, None

def run_single_check_via_checker(checker_path: Path, spec_file: Path, sop_file: Path) -> Tuple[bool, Optional[int]]:
    cp = subprocess.run([sys.executable, str(checker_path), str(spec_file), str(sop_file), "--quiet"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    return parse_checker_quiet_line(cp.stdout)

def main():
    parser = argparse.ArgumentParser(description="Run submissions on cases using checker.py --quiet and output CSV")
    parser.add_argument("--submissions-dir", required=True)
    parser.add_argument("--cases-dir", required=True)
    parser.add_argument("--time-limit", type=int, default=180)
    parser.add_argument("--out-csv", default="results.csv")
    parser.add_argument("--work-dir", default="")
    args = parser.parse_args()

    submissions_root = Path(args.submissions_dir).resolve()
    cases_dir = Path(args.cases_dir).resolve()
    submissions = discover_submissions(submissions_root)
    cases = discover_cases(cases_dir)
    case_labels = [p.stem for p in cases]

    checker_path = Path(__file__).with_name("checker.py").resolve()
    if not checker_path.exists():
        raise FileNotFoundError(f"{checker_path} not found")

    if args.work_dir:
        work_root = Path(args.work_dir).resolve()
        work_root.mkdir(parents=True, exist_ok=True)
        auto_cleanup = False
    else:
        work_root = Path(tempfile.mkdtemp(prefix="PA1_"))
        auto_cleanup = True

    results: Dict[str, Dict[str, Any]] = {get_student_id(sub.name): {} for sub in submissions}

    try:
        for sub in submissions:
            sid = get_student_id(sub.name)
            work_subdir = extract_tar_in_dir(sub)
            if work_subdir is None:
                for i, _ in enumerate(cases):
                    results[sid][case_labels[i]] = {"pass": "FAIL", "lit": "", "runtime": "", "rank": "", "score": 0}
                continue

            run_cmd(["make", "clean"], work_subdir, None)
            run_cmd(["make"], work_subdir, None)
            sop_path = work_subdir / "sop"
            if not sop_path.exists():
                for i, _ in enumerate(cases):
                    results[sid][case_labels[i]] = {"pass": "FAIL", "lit": "", "runtime": "", "rank": "", "score": 0}
                continue

            for i, spec_path in enumerate(cases):
                label = case_labels[i]
                out_dir = work_root / sid
                out_dir.mkdir(parents=True, exist_ok=True)
                out_sop = out_dir / f"{label}.sop"

                elapsed, _ = time_run_exec([str(sop_path), str(spec_path), str(out_sop)], work_subdir, args.time_limit)

                if out_sop.exists():
                    passed, lit = run_single_check_via_checker(checker_path, spec_path, out_sop)
                else:
                    passed, lit = False, None

                if passed:
                    results[sid][label] = {"pass": "PASS", "lit": lit, "runtime": round(elapsed, 4), "rank": None, "score": None}
                else:
                    results[sid][label] = {"pass": "FAIL", "lit": "" if lit is None else lit, "runtime": round(elapsed, 4), "rank": "", "score": 0}

        for i, label in enumerate(case_labels):
            case_data = []
            for sid in results:
                row = results[sid][label]
                row["submission"] = sid
                case_data.append(row)
            rank_and_score(case_data)

        fieldnames = ["submission"]
        for label in case_labels:
            fieldnames += [f"{label}_pass", f"{label}_lit", f"{label}_runtime", f"{label}_rank", f"{label}_score"]
        fieldnames.append("total_score")
        out_csv = Path(args.out_csv).resolve()
        with open(out_csv, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for sid in results:
                row = {"submission": sid}
                total = 0
                for label in case_labels:
                    d = results[sid][label]
                    row[f"{label}_pass"] = d["pass"]
                    row[f"{label}_lit"] = d["lit"]
                    row[f"{label}_runtime"] = d["runtime"]
                    row[f"{label}_rank"] = d["rank"]
                    row[f"{label}_score"] = d["score"]
                    total += d["score"] if d["score"] else 0
                row["total_score"] = round(total, 2)
                writer.writerow(row)
        print(f"[OK] CSV written to {out_csv}")
        print(f"[INFO] work dir = {work_root}")
    finally:
        if auto_cleanup:
            shutil.rmtree(work_root, ignore_errors=True)

if __name__ == "__main__":
    main()
