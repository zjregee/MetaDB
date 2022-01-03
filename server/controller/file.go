package controller

import (
	"MetaDB/server/tool"
	"MetaDB/server/tool/context"
	"MetaDB/server/model"

	"net/http"

	"github.com/labstack/echo/v4"
)

func UploadNewFile(c echo.Context) error {
	var u = struct {
		Key      string `json:"key"`
		Filename string `json:"filename"` 
	} {}

	err := c.Bind(&u)
	if err != nil {
		return err
	}
	
	uploadToken := tool.GenerateUUID()

	file := model.File {
		Token: uploadToken,
		Key: u.Key,
		FileName: u.Filename,
	}
	model.GFQ.Push(file)

	resData := map[string]interface{}{}
	resData["upload_token"] = uploadToken
	return context.RetData(c, resData)
}

func UploadFile(c echo.Context) error {
	var u = struct {
		Token      string `json:"token"`
	} {}

	err := c.Bind(&u)
	if err != nil {
		return err
	}

	fileIndex := model.GFQ.FindIndex(func(item model.File) bool {
		return item.Token == u.Token
	})
	file := model.GFQ.Get(fileIndex)

	// TODO: 下载文件放置在服务器固定路径

	UUID := tool.GenerateUUID()
	file.UUID = UUID
	file.Path = ""

	resData := map[string]interface{}{}
	return context.RetData(c, resData)
}

func DownloadFileByKey(c echo.Context) error {
	return c.String(http.StatusOK, "")
}

func DownloadFileByFileName(c echo.Context) error {
	return c.String(http.StatusOK, "")
}

func DeleteFileByKey(c echo.Context) error {
	return c.String(http.StatusOK, "")
}

func DeleteFileByFileName(c echo.Context) error {
	return c.String(http.StatusOK, "")
}

func ModifyKey(c echo.Context) error {
	return c.String(http.StatusOK, "")
}

func ModifyFileName(c echo.Context) error {
	return c.String(http.StatusOK, "")
}

func ModifyFile(c echo.Context) error {
	return c.String(http.StatusOK, "")
}

func GetAllFileInfo(c echo.Context) error {
	return c.String(http.StatusOK, "")
}