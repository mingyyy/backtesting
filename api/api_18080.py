import requests


path='http://ec2-3-231-23-67.compute-1.amazonaws.com:18080/api/v1/applications/'
app_id = 'app-20190925195753-0058'
action = 'jobs'
path += app_id + '/' + action + '/'

response = requests.get(path)
print(response)
