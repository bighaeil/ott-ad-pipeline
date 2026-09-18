package com.ottads.collector.service

import com.ottads.collector.config.CollectorProperties
import org.springframework.stereotype.Component
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.time.Instant
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec

/**
 * VAST 트래킹 픽셀 서명 검증.
 *
 * canonical string = sig 를 제외한 모든 쿼리 파라미터를 key 오름차순으로 정렬해
 *                    "k=v" 로 만들고 "&" 로 이은 문자열 (값은 URL 디코딩된 상태)
 * sig             = HMAC-SHA256(secret, canonical) 을 소문자 hex 로
 *
 * 운영에서는 secret 을 캠페인/파트너별로 나누고 키 롤링을 하지만
 * 여기서는 단일 공유키로 축소했다.
 */
@Component
class SignatureVerifier(private val props: CollectorProperties) {

    sealed interface Result {
        data object Valid : Result
        data class Invalid(val reason: String) : Result
    }

    fun verify(params: Map<String, List<String>>): Result {
        val sig = params["sig"]?.firstOrNull()
            ?: return Result.Invalid("missing_sig")
        val expRaw = params["exp"]?.firstOrNull()
            ?: return Result.Invalid("missing_exp")

        val exp = expRaw.toLongOrNull() ?: return Result.Invalid("bad_exp")
        if (Instant.now().epochSecond > exp) return Result.Invalid("expired")

        val expected = sign(canonical(params))
        // 타이밍 공격 방지를 위해 상수시간 비교
        val ok = MessageDigest.isEqual(
            expected.toByteArray(StandardCharsets.UTF_8),
            sig.lowercase().toByteArray(StandardCharsets.UTF_8),
        )
        return if (ok) Result.Valid else Result.Invalid("bad_signature")
    }

    fun canonical(params: Map<String, List<String>>): String =
        params.entries
            .filter { it.key != "sig" }
            .sortedBy { it.key }
            .joinToString("&") { (k, v) -> "$k=${v.firstOrNull() ?: ""}" }

    fun sign(canonical: String): String {
        val mac = Mac.getInstance("HmacSHA256")
        mac.init(SecretKeySpec(props.hmacSecret.toByteArray(StandardCharsets.UTF_8), "HmacSHA256"))
        return mac.doFinal(canonical.toByteArray(StandardCharsets.UTF_8))
            .joinToString("") { "%02x".format(it) }
    }
}
