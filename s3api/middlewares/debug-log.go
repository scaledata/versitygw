// Copyright 2023 Versity Software
// This file is licensed under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package middlewares

import (
	"fmt"
	"log"
	"net/http"
	"strings"

	"github.com/gofiber/fiber/v3"
	"github.com/versity/versitygw/debuglogger"
)

// These live here rather than in debuglogger so that package's only
// dependencies stay on the standard library. They are the sole reason
// debuglogger would otherwise import fiber, and consumers that vendor it for
// its plain string loggers would then pull in the whole fiber/fasthttp stack
// for code they can never reach.

// LogFiberRequestDetails logs http request details: headers, body, params, query args
func LogFiberRequestDetails(ctx fiber.Ctx) {
	// Log the full request url
	fullURL := ctx.Scheme() + "://" + ctx.Host() + ctx.OriginalURL()
	fmt.Printf("%s[URL]: %s%s\n", debuglogger.Green, fullURL, debuglogger.Reset)

	// log request headers
	debuglogger.WrapInBox(debuglogger.Green, "REQUEST HEADERS", func() {
		for key, value := range ctx.Request().Header.All() {
			debuglogger.PrintWrappedLine(debuglogger.Yellow, string(key), string(value))
		}
	})
	// skip request body log for PutObject and UploadPart
	skipBodyLog := isLargeDataAction(ctx)
	if !skipBodyLog {
		body := ctx.Request().Body()
		if len(body) != 0 {
			debuglogger.PrintBoxTitleLine(debuglogger.Blue, "REQUEST BODY", false)
			fmt.Printf("%s%s%s\n", debuglogger.Blue, body, debuglogger.Reset)
			debuglogger.PrintHorizontalBorder(debuglogger.Blue, false)
		}
	}

	if ctx.Request().URI().QueryArgs().Len() != 0 {
		for key, value := range ctx.Request().URI().QueryArgs().All() {
			log.Printf("%s: %s", key, value)
		}
	}
}

// LogFiberResponseDetails logs http response details: body, headers
func LogFiberResponseDetails(ctx fiber.Ctx) {
	debuglogger.WrapInBox(debuglogger.Green, "RESPONSE HEADERS", func() {
		for key, value := range ctx.Response().Header.All() {
			debuglogger.PrintWrappedLine(debuglogger.Yellow, string(key), string(value))
		}
	})

	_, ok := ctx.Locals("skip-res-body-log").(bool)
	if !ok {
		body := ctx.Response().Body()
		if len(body) != 0 {
			debuglogger.PrintInsideHorizontalBorders(debuglogger.Blue, "RESPONSE BODY", string(body), debuglogger.BoxWidth)
		}
	}
}

// TODO: remove this and use utils.IsBidDataAction after refactoring
// and creating 'internal' package
func isLargeDataAction(ctx fiber.Ctx) bool {
	pathParts := strings.Split(ctx.Path(), "/")

	// PutObject and UploadPart
	if ctx.Method() == http.MethodPut && len(pathParts) >= 3 {
		if !ctx.Request().URI().QueryArgs().Has("tagging") && ctx.Get("X-Amz-Copy-Source") == "" && !ctx.Request().URI().QueryArgs().Has("acl") {
			return true
		}
	}

	isBucketAction := (len(pathParts) == 3 && pathParts[2] == "") || (len(pathParts) == 2 && pathParts[1] != "")

	// POST object action
	if isBucketAction && ctx.Method() == http.MethodPost {
		return true
	}

	return false
}
