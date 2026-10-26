package vai.lakehouse.dakmock.web;

import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

/** Gắn {@link PortalAuthInterceptor} vào {@code /mock-portal/**}; các endpoint khác của mock không bị ảnh hưởng. */
@Configuration
public class PortalWebConfig implements WebMvcConfigurer {

  private final PortalAuthInterceptor portalAuth;

  public PortalWebConfig(PortalAuthInterceptor portalAuth) {
    this.portalAuth = portalAuth;
  }

  @Override
  public void addInterceptors(InterceptorRegistry registry) {
    registry.addInterceptor(portalAuth).addPathPatterns("/mock-portal/**");
  }
}
