package main

import (
	"MetaDB/server/controller"
	"MetaDB/server/tool/log"
	"MetaDB/server/model"

	"github.com/labstack/echo/v4"
)

func main() {
	log.InitLog()
	defer log.CloseLog()
	model.InitGlobalFileQueue()

	e := echo.New()

	login := e.Group("/login")
	login.POST("/login", controller.Login)

	file := e.Group("/file")
	file.POST("/new", controller.UploadNewFile)
	file.POST("/upload", controller.UploadFile)
	file.POST("/downloadbykey", controller.DownloadFileByKey)
	file.POST("/downloadbyfilename", controller.DownloadFileByFileName)
	file.POST("/deletebykey", controller.DeleteFileByKey)
	file.POST("/deletebyfilename", controller.DeleteFileByFileName)
	file.POST("/modifykey", controller.ModifyKey)
	file.POST("/modifyfilename", controller.ModifyFileName)
	file.POST("/modifyfile", controller.ModifyFile)
	file.GET("/getall", controller.GetAllFileInfo)

	e.Logger.Fatal(e.Start(":8080"))
}