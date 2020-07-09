import argparse
import os
import fnmatch
import concurrent.futures

from tqdm import tqdm

opt = argparse.ArgumentParser()
opt.add_argument('tar_dir', help="Directory of tar files")
opt.add_argument('out_dir', help="Directory of output files")
opt.add_argument('--debug', action='store_true', help="Debug mode.")
opt = vars(opt.parse_args())


def tar_job(job):
    os.system(f"tar -xf {job['tar_path']} -C {job['out_dir']}")


if __name__ == '__main__':
    tar_files = fnmatch.filter(os.listdir(opt['tar_dir']), '*.tar')
    if opt['debug']:
        tar_files = tar_files[:2]

    jobs = []
    for tf in tar_files:
        class_name = tf.split('.')[0]
        tar_path = os.path.join(opt['tar_dir'], tf)
        out_dir = os.path.join(opt['out_dir'], class_name)
        if not os.path.exists(out_dir):
            os.makedirs(out_dir)
            jobs.append({'tar_path': tar_path, 'out_dir': out_dir})

    with concurrent.futures.ThreadPoolExecutor(max_workers=12) as executor:
        # wrap with list to run .map generator on execution
        _ = list(tqdm(executor.map(tar_job, jobs), total=len(jobs)))
