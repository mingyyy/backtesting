import requests


def check_jobs(app_id, action):
    path = 'http://ec2-3-231-23-67.compute-1.amazonaws.com:18080/api/v1/applications/'
    path += app_id + '/' + action + '/'

    response = requests.get(path)
    if response.status_code != 200:
        return print(response.status_code)
    else:
        jobs = response.json()
        for job in jobs:
            if job['status'] != 'SUCCEEDED':
                return print('JobID:', job['jobId'], job['status'])
        return print('All jobs succeeded!')


def check_stages(app_id, action):
    path = 'http://ec2-3-231-23-67.compute-1.amazonaws.com:18080/api/v1/applications/'
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
    check_jobs('app-20190925231336-0062','jobs')
    check_stages('app-20190925231336-0062', 'stages')
