import isb_cgc_user_data.uduprocessor

def processUserData(user_data_config, success_url, failure_url):
    isb_cgc_user_data.uduprocessor.process_upload(user_data_config, success_url, failure_url)
    return "Success"

def ping_the_pipe():
    print ("Pipe pinged")
    return "Success"
