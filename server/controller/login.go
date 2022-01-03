package controller

import (
	"github.com/labstack/echo/v4"

	"net/http"
)

func Login(c echo.Context) error {
	return c.String(http.StatusOK, "")
}