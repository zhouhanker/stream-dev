package main

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/wangluozhe/requests"
	"github.com/wangluozhe/requests/url"
	"net/http"
)

type T struct {
	Http  string `json:"http"`
	Https string `json:"https"`
}

type Login struct {
	UrlStr     string `form:"url" json:"url" binding:"required"`
	Ja3Str     string `form:"ja3" json:"ja3" binding:"required"`
	ProxiesStr T      `form:"ProxiesStr" json:"ProxiesStr" binding:"required"`
}

func Ja3request(urlS, j3 string, Proxies ...string) (string, int, error) {
	req := url.NewRequest()
	headers := url.NewHeaders()
	headers.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.198 Safari/537.36")
	req.Headers = headers
	req.Ja3 = j3
	if Proxies != nil {
		req.Proxies = Proxies[0]
	}

	r, err := requests.Get(urlS, req)
	if err != nil {
		return "", 0, err
	}
	fmt.Println(r.StatusCode)
	return r.Text, r.StatusCode, nil

}

func main() {
	r := gin.Default()
	r.POST("/", func(c *gin.Context) {
		var login Login
		if err := c.ShouldBind(&login); err == nil {
			html, StatusCode, err := Ja3request(login.UrlStr, login.Ja3Str, login.ProxiesStr.Http)
			if err != nil {
				c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			} else {
				c.JSON(http.StatusOK, gin.H{
					"HTML": html,
					"code": StatusCode,
				})
			}

		} else {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		}
	})

	err := r.Run(":55530")
	if err != nil {
		return
	}
}
