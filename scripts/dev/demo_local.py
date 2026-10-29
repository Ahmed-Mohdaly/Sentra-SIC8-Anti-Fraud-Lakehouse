#!/usr/bin/env python3
"""Run deterministic local fixture without cloud writes or live model claims."""
import argparse,json
from src.common.pipeline import run_local

def main():
    p=argparse.ArgumentParser();p.add_argument('--output',default='build/demo');p.add_argument('--seed',type=int,default=8)
    args=p.parse_args()
    print(json.dumps(run_local(args.output,seed=args.seed),indent=2))
if __name__=='__main__':main()
