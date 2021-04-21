# Azure EventHub Consumer
This is a simple utility to connect to an Azure EventHub and stream messages to standard out
## Dependencies
* Azure account
* Node
* VS Code (not required but great)
## Install
* Clone this repo and `npm install`
* Create a `.env` file at the project root and add the following values:
  ```
  ehConnectionString=<EVENTHUB_CONNECTIONSTRING>
  ehConsumerGroup=$Default
  ehName=<EVENTHUB_NAME>
  saConnectionString=<STORAGE_ACCOUNT_CONNECTION_STRING>
  saContainerName=<BLOB_CONTAINER_NAME>
  matchValues=<OPTIONAL>
  ```
* Run the code
  ```
  npm run build
  node ./dist/index.js
  ```
* Or, in VS Code just press F5
