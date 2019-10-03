import requests


def check_jobs(path, app_id, action):
    path += app_id + '/' + action + '/'

    response = requests.get(path)
    if response.status_code != 200:
        return print(response.status_code)
    else:
        jobs = response.json()
        for job in jobs:
            if job['status'] != 'SUCCEEDED':
                return print('JobID:', job['jobId'], job['status'])
        return 'ok'


def check_stages(path, app_id, action):
    path += app_id + '/' + action + '/'

    response = requests.get(path)
    if response.status_code != 200:
        return print(response.status_code)
    else:
        stages = response.json()
        for stage in stages:
            if stage['status'] != 'COMPLETED':
                str='attemptID: {} failed: {}'.format(stage['attemptId'], stage['failureReason'])
                return print('StageID:', stage['stageId'], stage['status'], str)
        return print('All stages completed!')


if __name__ == '__main__':
    app_id = 'app-20191001162803-0177'
    host = 'ec2-54-89-132-244.compute-1.amazonaws.com'
    path = 'http://{}:18080/api/v1/applications/'.format(host)
    if check_jobs(path, app_id,'jobs') == 'ok':
        print('All jobs succeeded!')
    else:
        check_stages(path, app_id, 'stages')
