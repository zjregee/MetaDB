## Server API 文档

### 上传新文件

POST /file/new

请求: JSON body

```json
{
  "key": "",
  "filename": ""
}
```

响应:

```json
{
  "status": 200,
  "data": {
    "upload_token": ""
  }
}
```

### 上传文件

POST /file/upload

请求: JSON body

```json
{
  "upload_token": ""
}
```

响应:

```json
{
  "status": 200,
  "data": {
    "success": 1
  }
}
```

### 通过key下载文件 POST

POST /file/downloadbykey

请求: JSON body

```json
{
  "key": ""
}
```

响应:

```json
```

### 通过filename下载文件 POST

POST /file/downloadbyfilename

请求: JSON body

```json
{
  "filename": ""
}
```

响应:

```json

```

### 通过key删除文件

POST /file/deletebykey

请求: JSON body

```json
{
  "key": ""
}
```

响应:

```json

```

### 通过filename删除文件

POST /file/deletebyfilename

请求: JSON body

```json
{
  "filename": ""
}
```

响应:

```json

```

### 修改文件的key

POST /file/modifykey

请求: JSON body

```json
```

响应:

```json
```

### 修改文件的filename

POST /file/modifyfilename

请求: JSON body

```json

```

响应:

```json

```

### 更新文件

POST /file/modifyfile

请求: JSON body

```json

```

响应:

```json

```

### 查看所有文件

GET /file/getall

请求: null

响应:

```json
{
  "status": 200,
  "data": [
    {
      "key": "",
      "filename": ""
    },
    {
      "key": "",
      "filename": ""
    }
  ]
}
```