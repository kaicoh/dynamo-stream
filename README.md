# dynamo-stream

Extract dynamodb stream and send it anywhere you like.
You can configure what table to subscribe and where to send dynamodb stream records.

## Docker image

You can use docker image from [this link](https://hub.docker.com/r/kaicoh/dynamo-stream).

## How to use

Set environment variable `DYNAMODB_ENDPOINT_URL` and start the server. The following is an example using docker compose.
You can register what table to subscribe and where to send extracted dynamodb records both statically and dynamically.

```
version: "3"

services:
  stream:
    image: kaicoh/dynamo-stream:latest
    ports:
      - 3000:3000
    environment:
      - DYNAMODB_ENDPOINT_URL=http://db:8000
      - AWS_ACCESS_KEY_ID=test
      - AWS_SECRET_ACCESS_KEY=test
      - AWS_DEFAULT_REGION=ap-northeast-1

  db:
    image: public.ecr.aws/aws-dynamodb-local/aws-dynamodb-local:latest
    ports:
      - 8000:8000
```

### Static Configuration

You can register data source and destination using configuration file.
The configuration file is a yaml file like the following.

```
entries
    # Each entry must have `table_name` to subscribe and `url` to which the POST request send.
  - table_name: People
    url: http://localhost:9000/streams

  - table_name: AnyTable
    url: https://example.com
```

The dynamo-stream can read this configuration file by passed environment variable `CONFIG_PATH`.

```
version: "3"

services:
  stream:
    image: kaicoh/dynamo-stream:latest
    ports:
      - 3000:3000

    #####################################################
    # Mount your configuration file to docker container
    #####################################################
    volumes:
      - ./configs/config.yml:/app/configs/config.yml

    environment:
      - DYNAMODB_ENDPOINT_URL=http://db:8000
      - AWS_ACCESS_KEY_ID=test
      - AWS_SECRET_ACCESS_KEY=test
      - AWS_DEFAULT_REGION=ap-northeast-1
      ##########################################################################
      # Set path to the configuration file as environment variable CONFIG_PATH
      ##########################################################################
      - CONFIG_PATH=/app/configs/config.yml

  db:
    image: public.ecr.aws/aws-dynamodb-local/aws-dynamodb-local:latest
    ports:
      - 8000:8000
```

### Dynamic Configuration

The alternate of configuration file is executing http request. For example, if you want to subscribe `People` table and send records to http://localhost:9000/stream, execute POST request with JSON payload. This example is assuming the dynamo-stream server is running on localhost:3000.

```
$ curl -X POST \
  -H 'Content-Type: application/json' \
  -d '{"table_name":"People","url":"http://localhost:9000/stream"}' \
  http://localhost:3000
```

And you can also confirm current state via http request.

```
$ curl -s http://localhost:3000 | jq .
{
  "01HEWN8M2PVGCA1R8SAY735E9S": {
    "table_name": "People",
    "url": "http://localhost:9000/stream",
    "status": "RUNNING"
  }
}
```

### Environment variables

The environment variables this app can recognize are the followings.

| name | value |
----|----
| DYNAMODB_ENDPOINT_URL | The endpoint url to dynamodb |
| PORT | The port number this app runs on |
| CONFIG_PATH | The path to configuration file |

And you can also use any other variables that AWS SDK uses, like `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` and `AWS_DEFAULT_REGION`.

## Subscription payload

When the dynamo-stream get records form dynamodb stream, it sends that records via http POST request with JSON payload. The format of the payload is the same of what the actual AWS Dynamodb stream sends to any other AWS services [like this](https://docs.aws.amazon.com/lambda/latest/dg/with-ddb-example.html#with-dbb-invoke-manually).

### When something went wrong

If any errors occur while the subscription, dynamo-stream sends the error message to the destination url and quit the subscription. The error message payload is following.

```
{
   "table_name": "People"
   "message": "Something went wrong and the subscription to the table is discarded.",
}
```

The details of the error are emitted to the logs of dynamo-stream server, and you can also confirm the subscription status via http request.

```
$ curl -s http://localhost:3000 | jq .
{
  "01HEWN8M2PVGCA1R8SAY735E9S": {
    "table_name": "People",
    "url": "http://localhost:9000/stream",
    "status": "ERROR",
    "error": "Any error messages are here"
  }
}
```

## License

This software is released under the [MIT License](LICENSE).
