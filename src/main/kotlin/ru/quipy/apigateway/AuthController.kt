package ru.quipy.apigateway

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RestController

@RestController
class AuthController {

    val logger: Logger = LoggerFactory.getLogger(AuthController::class.java)

    @PostMapping("/authentication")
    fun authentication(@RequestBody jsonString: String): TokenResponse {
        return TokenResponse("accessToken", "refreshToken")
    }

    @PostMapping("/authentication/refresh")
    fun authenticationRefresh(@RequestBody jsonString: String): TokenResponse {
        return TokenResponse("accessToken", "refreshToken")
    }

    data class TokenResponse(val accessToken: String, val refreshToken: String)
}