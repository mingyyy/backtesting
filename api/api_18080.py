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
        return 'ok'


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


def check_tasks(app_id, action):
    path = 'http://ec2-3-231-23-67.compute-1.amazonaws.com:18080/api/v1/applications/'
    path += app_id + '/' + action + '/'

    response = requests.get(path)
    if response.status_code != 200:
        return print(response.status_code)
    else:
        tasks = response.json()
        for task in tasks:
            print(task)
        #     if task['status'] != 'COMPLETED':
        #         str='attemptID: {} failed: {}'.format(task['attemptId'], task['failureReason'])
        #         return print('StageID:', task['stageId'], task['status'], str)
        # return print('All stages completed!')


if __name__ == '__main__':
    app_id = 'app-20190930134723-0167'
    if check_jobs(app_id,'jobs') == 'ok':
        print('All jobs succeeded!')
    else:
        check_stages(app_id, 'stages')

    # check_tasks('app-20190926015654-0064', 'tasks')
