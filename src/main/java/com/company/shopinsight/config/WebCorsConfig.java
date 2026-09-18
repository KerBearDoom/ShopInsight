package com.company.shopinsight.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

/**
 * 跨域配置。
 *
 * <p>为什么需要：Flutter Web 跑在浏览器里，访问后端属于跨源请求。实测不加这个配置时：
 * <ul>
 *   <li>{@code OPTIONS} 预检请求返回 <b>403</b></li>
 *   <li>{@code GET} 响应头里没有 {@code Access-Control-Allow-Origin}，浏览器直接拦掉</li>
 * </ul>
 * 结果是 Flutter Web 一个请求都发不出去。
 *
 * <p><b>为什么用 {@code allowedOriginPatterns} 而不是 {@code allowedOrigins}：</b>
 * {@code flutter run -d chrome} 每次分配的端口是随机的（如 localhost:53211），
 * 写死端口意味着每次重启都要改代码。用通配模式一次配好。
 *
 * <p>注意这只是开发配置 —— 生产环境应该限定具体域名。
 */
@Configuration
public class WebCorsConfig implements WebMvcConfigurer {

    @Override
    public void addCorsMappings(CorsRegistry registry) {
        registry.addMapping("/api/**")
                .allowedOriginPatterns("http://localhost:*", "http://127.0.0.1:*")
                .allowedMethods("GET", "OPTIONS")
                .allowedHeaders("*")
                .maxAge(3600);
    }
}
