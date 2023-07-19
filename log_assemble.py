import argparse
import heapq
import json


def merge_logs(log1, log2, output):
    def load_jsonl(file):
        with open(file) as f:
            for line in f:
                yield json.loads(line)

    gen1 = load_jsonl(log1)
    gen2 = load_jsonl(log2)

    with open(output, 'w') as f:
        for row in heapq.merge(gen1, gen2, key=lambda x: x['timestamp']):
            f.write(json.dumps(row))
            f.write('\n')


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('log1', type=str, help='Path to first log file')
    parser.add_argument('log2', type=str, help='Path to second log file')
    parser.add_argument('-o', '--output', type=str, help='Path to output file')
    return parser.parse_args()


def main():
    args = parse_args()
    merge_logs(args.log1, args.log2, args.output)


if __name__ == '__main__':
    main()