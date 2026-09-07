"""Plot measured load samples; run with an isolated matplotlib dependency (see capacity.md)."""
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt


def memory_gib(text):
    match = re.match(r'([0-9.]+)([kKMGT]?i?B)', text.split('/')[0].strip())
    if not match:
        raise ValueError('Unknown Docker memory unit: ' + text)
    value, unit = match.groups()
    factors = {'B':1, 'KiB':1024, 'MiB':1024**2, 'GiB':1024**3, 'TiB':1024**4,
               'kB':1000, 'KB':1000, 'MB':1000**2, 'GB':1000**3, 'TB':1000**4}
    return float(value) * factors[unit] / 1024**3


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('reports', nargs='+', type=Path)
    parser.add_argument('--output', type=Path, default=Path('docs/evidence/operations/load-timeline.png'))
    args=parser.parse_args()
    fig, axes=plt.subplots(4, len(args.reports), figsize=(7*len(args.reports), 12), squeeze=False, sharex='col')
    plt.rcParams.update({'font.size':10})
    for col, path in enumerate(args.reports):
        report=json.loads(path.read_text())
        samples=[json.loads(line) for line in path.with_suffix('.samples.jsonl').read_text().splitlines()]
        samples=[s for s in samples if 'error' not in s]
        minutes=[s['elapsed_seconds']/60 for s in samples]
        axes[0,col].plot(minutes,[s['interval_events_per_second'] for s in samples], color='#1463ad', marker='.', label='Observed interval rate')
        axes[0,col].axhline(report['target_rate'], color='#555', linestyle='--', label='Target')
        axes[0,col].set_title(f"{report['target_rate']:,} events/s target · {report['duration_seconds']/60:.1f} min input\n"
                              f"{report['accepted']:,} receipts · final P95 {report['freshness']['p95_seconds']:.3f}s")
        axes[0,col].set_ylabel('Accepted events / second')
        axes[1,col].plot(minutes,[s['freshness']['p95_seconds'] for s in samples], color='#19856d', label='Cumulative visible-subset P95')
        axes[1,col].axhline(report['freshness_slo_seconds'], color='#b35125', linestyle='--', label='60s freshness target')
        axes[1,col].scatter([report['duration_seconds']/60],[report['freshness']['p95_seconds']], color='#111', label='Final all-receipt P95', zorder=3)
        axes[1,col].set_ylim(bottom=0)
        axes[1,col].set_ylabel('Receipt-to-visible seconds')
        axes[2,col].plot(minutes,[s['pending_visibility'] for s in samples], color='#8654a2', label='Accepted minus visible')
        axes[2,col].set_ylabel('Pending visibility (receipts)')
        memory=[sum(memory_gib(c['MemUsage']) for c in s['containers'] if 'taskmanager-' in c['Name']) for s in samples]
        state=[sum(j['checkpoint_bytes'] or 0 for j in s['jobs'].values())/1024**3 for s in samples]
        axes[3,col].plot(minutes,memory,color='#1463ad',label='Two TaskManagers: Docker memory')
        axes[3,col].plot(minutes,state,color='#b35125',label='REST latest checkpoint state_size sum')
        axes[3,col].set_ylabel('GiB (different measurements)')
        axes[3,col].set_xlabel('Elapsed input time (minutes)')
        for row in range(4):
            axes[row,col].grid(alpha=0.2)
            axes[row,col].legend(loc='best',fontsize=8)
            axes[row,col].set_xlim(0,report['duration_seconds']/60)
    fig.suptitle('AdPulse · actual local Docker load experiments\nShared physical host; synthetic sessions; one run per target; no 26-hour capacity inference',fontsize=14)
    fig.tight_layout(rect=(0,0,1,0.95))
    args.output.parent.mkdir(parents=True,exist_ok=True)
    fig.savefig(args.output,dpi=150)
    fig.savefig(args.output.with_suffix('.pdf'))
    print(args.output)


if __name__=='__main__':
    main()
