import os
import subprocess
import time
from tqdm import tqdm
from main import stocks, is_file_over_3_days_old
import itertools

def edgar_download():
    datadir = os.path.expanduser("~/Dropbox/Family Room/data")
    repo_path = "."

    pbar = tqdm(itertools.islice(stocks(), 2))
    for stock in pbar:
        pbar.set_description(stock['Symbol'])
        symbol = stock['Symbol'].replace('.', '-')

        if stock['CIK'] is None:
            continue

        cik = "%010d" % stock['CIK']

        file = os.path.join(
            datadir,
            f"data-sec-edgar/edgar/{symbol}.json"
        )

        if os.path.exists(file) and not is_file_over_3_days_old(file):
            continue

        # Download the file
        download_command = f"curl -H 'User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36' https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json -o '{file}'"
        print(subprocess.getoutput(download_command))

        # Change the working directory to the Git repository root
        os.chdir(repo_path)

        # Add the downloaded file to the Git repository
        git_add_command = f"git add {file}"
        subprocess.getoutput(git_add_command)

        # Commit the changes with a message
        commit_message = f"Added {symbol}.json"
        git_commit_command = f"git commit -m '{commit_message}'"
        subprocess.getoutput(git_commit_command)

        # Push the changes to the remote Git repository
        git_push_command = f"git push origin master"  # Modify the branch name if needed
        subprocess.getoutput(git_push_command)

        # Change the working directory back to the original directory
        os.chdir(datadir)

        time.sleep(2)

if __name__ == '__main__':
    edgar_download()
