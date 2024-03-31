import asyncio
import csv
import os
from netmiko import ConnectHandler
import datetime
from urllib.parse import urlparse
import logging
import sys
import subprocess
import re
from logging.handlers import TimedRotatingFileHandler

MAX_CONCURRENT_BACKUPS = 10
semaphore = asyncio.Semaphore(MAX_CONCURRENT_BACKUPS)

def setup_logging(log_file):
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    error_handler = TimedRotatingFileHandler(filename=log_file,
                                             when='midnight',
                                             interval=1,
                                             backupCount=7)
    error_handler.setLevel(logging.WARNING)
    error_handler.suffix = "%Y%m%d.log"

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    error_handler.setFormatter(formatter)

    root_logger.addHandler(error_handler)

async def clone_repo(gitlab_url, script_dir, pro_dir):
    os.chdir(script_dir)
    if not os.path.exists(pro_dir):
        try:
            process = await asyncio.create_subprocess_shell(
                f'git clone {gitlab_url}',
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE)
            stdout, stderr = await process.communicate()

            if process.returncode != 0:
                logging.error(f'Error cloning repository: {stderr.decode()}')
                sys.exit(1)
            else:
                logging.info('Repository cloned successfully')
        except Exception as e:
            logging.error(f'Error cloning repository: {e}')
            sys.exit(1)

async def backup_config(ip, username, password, platform, role, script_dir, pro_dir):
    max_retries = 5
    async with semaphore:
        for retry_count in range(max_retries):
            try:
                device = {
                    'device_type': platform,
                    'ip': ip,
                    'username': username,
                    'password': password,
                    'session_log': f'{script_dir}/logs/{platform}-{role}-{ip}-session.log',
                }

                with ConnectHandler(**device) as conn:
                    if "Y/N" in conn.find_prompt():
                        conn.send_command_expect('N', expect_string='>')
                        if platform == 'hp_comware':
                            conn.send_command_expect('screen-length disable', expect_string='>')
                        elif platform == 'huawei':
                            conn.send_command_expect('screen-length 0 temporary', expect_string='>')
                        elif platform == 'ruijie_os':
                            conn.send_command_expect('terminal width 256', expect_string='#')
                            conn.send_command_expect('terminal length 0', expect_string='#')

                    if platform == 'ruijie_os':
                        output = conn.send_command('show running-config', read_timeout=1800)
                    else:
                        output = conn.send_command('display current', read_timeout=1800)

                filename = f'{platform}-{role}-{ip}-config.txt'
                with open(os.path.join(pro_dir, filename), 'w+') as f:
                    f.write(output)
                logging.info(f'Config saved to {filename}')
                await git_commit_and_push(filename, pro_dir)
                break
            except Exception as e:
                logging.error(f'Error backing up {ip}: {e}')
                if retry_count < max_retries - 1:
                    logging.warning(
                        f'Retrying backup for {ip} ({retry_count+1}/{max_retries})'
                    )
                else:
                    logging.error(f'Max retries reached for {ip}, backup failed.')

async def git_commit_and_push(filename, pro_dir):
    os.chdir(pro_dir)
    try:
        add_command = f'git add {filename}'
        commit_command = f'git commit -m "backup {filename} {datetime.datetime.now()}"'
        push_command = 'git push'

        add_process = subprocess.run(add_command, shell=True, capture_output=True, text=True)
        if add_process.returncode != 0:
            logging.error(
                f"Error executing Git command '{add_command}'. Return code: {add_process.returncode}, Error: {add_process.stderr.strip()}"
            )

        commit_process = subprocess.run(commit_command, shell=True, capture_output=True, text=True)
        commit_result = re.sub(r'\n+', ',', commit_process.stdout.strip())
        if 'nothing to commit' in commit_result:
            logging.warning(
                f"Nothing to commit in the repository,{filename} is Already exists."
            )
        elif f'backup {filename}' in commit_result:
            logging.info(f'{filename} Submitted successfully')
        else:
            logging.error(f'{filename} Submission Failed')

        push_process = subprocess.run(push_command, shell=True, capture_output=True, text=True)
        if push_process.returncode != 0:
            logging.error(
                f"Error executing Git command '{push_command}'. Return code: {push_process.returncode}, Error: {push_process.stderr.strip()}"
            )

    except Exception as e:
        logging.error(f'Error committing and pushing: {e}')
    finally:
        os.chdir('..')

async def main():
    gitlab_url = 'https://username:password@ip:port/network/xxx.git'
    parsed_url = urlparse(gitlab_url)
    path_parts = parsed_url.path.split('/')
    pdir = path_parts[-1].split('.')[0]
    script_dir = os.path.dirname(os.path.abspath(__file__))
    pro_dir = os.path.join(script_dir, pdir)

    setup_logging(f'{script_dir}/logs/backup-err.log')
    await clone_repo(gitlab_url, script_dir, pro_dir)

    with open(f'{script_dir}/devices.csv', 'r', encoding='utf-8-sig') as file:
        reader = csv.DictReader(file)
        tasks = []
        for row in reader:
            ip, username, password, platform, role = row['ip'], row['username'], row['password'], row['platform'], row['role']
            tasks.append(backup_config(ip, username, password, platform, role, script_dir, pro_dir))

        await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())
