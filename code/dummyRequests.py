# Code from https://dev.to/21alul21/working-with-apis-in-python-a-practical-guide-3cok
import requests as rq

# Making a simple get
def simpleGet():
    response_object = rq.get("https://jsonplaceholder.typicode.com/posts")
    json_data = response_object.json()
    print("GET RESPONSE DATA:")
    print(json_data)

# Making a simple POST request

def simplePost():
    data = {'userId': 1, 'id': 1, 'title': 'This is for POST request', \
            'body': 'This body is modified for this technical writing article by Augustine Alul'}
    response_object = rq.post("https://jsonplaceholder.typicode.com/posts/", data=data)
    
    print("\nPOST RESPONSE STATUS CODE:")
    print(response_object.status_code)


# Making a simple PUT request

def simplePut():
    data = {'userId': 1, 'id': 1, 'title': 'This is for PUT', \
        'body': 'This body is modified for this technical writing article by Augustine Alul'}
    response_object = rq.put("https://jsonplaceholder.typicode.com/posts/1", data=data)

# Making a simple DELETE request

def simpleDelete():
    response_object = rq.delete("https://jsonplaceholder.typicode.com/posts/1")

# Making a simple PATCH request

def simplePatch():
    data = {'userId': 1, 'id': 1, 'title': 'This is for PATCH', \
        'body': 'This body is modified for this technical writing article by Augustine Alul'}
    response_object = rq.patch("https://jsonplaceholder.typicode.com/posts/1", data=data)


if __name__ == "__main__":
    simpleGet()
    simplePost()
    simplePut()
    simpleDelete()
    simplePatch()
