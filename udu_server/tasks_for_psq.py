import isb_cgc_user_data.uduprocessor

def processUserData():
    isb_cgc_user_data.uduprocessor.process_upload('config.json')
    return "Success"

def ping_the_pipe():
    print ("Pipe pinged")
    return "Success"
