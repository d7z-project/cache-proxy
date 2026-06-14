import { InstanceSummary, ProxyMode } from '../core/api.models';

export interface InstanceGuideSnippet {
  id: string;
  label: string;
  description: string;
  code: string;
}

export interface InstanceGuide {
  entryLabel: string;
  entryUrl?: string;
  snippets: InstanceGuideSnippet[];
}

export function buildInstanceGuide(instance: InstanceSummary): InstanceGuide {
  const entry = proxyEntry(instance);
  switch (instance.mode) {
    case ProxyMode.Go:
      return {
        entryLabel: entry.label,
        entryUrl: entry.url,
        snippets: [
          guideSnippet('go', 'go', '临时指定 GOPROXY，适合先验证模块下载是否能正常经过当前代理。', `GOPROXY=${entry.url} go mod download`)
        ]
      };
    case ProxyMode.Maven:
      return {
        entryLabel: entry.label,
        entryUrl: entry.url,
        snippets: [
          guideSnippet('maven-xml', 'Maven', '将仓库片段加入 pom.xml 的 repositories 节点，用于常规 Maven 依赖下载。', `<repository>\n  <id>${instance.name}</id>\n  <url>${entry.url}</url>\n</repository>`),
          guideSnippet('gradle', 'Gradle', '将仓库片段加入 build.gradle 或 settings.gradle 的 repositories 块。', `repositories {\n  maven(url = "${entry.url}")\n}`)
        ]
      };
    case ProxyMode.Cargo:
      return {
        entryLabel: entry.label,
        entryUrl: entry.url,
        snippets: [
          guideSnippet('cargo-config', 'Cargo', '写入 .cargo/config.toml，使用 sparse registry 通过当前代理下载 crate。', `[registries.${instance.name}]\nprotocol = "sparse"\nindex = "${entry.url}"`)
        ]
      };
    case ProxyMode.PyPI:
      return {
        entryLabel: entry.label,
        entryUrl: joinUrlPath(entry.url, 'simple'),
        snippets: [
          guideSnippet('pip', 'pip', '单次安装时临时指定 index-url，适合先验证请求是否命中当前代理。', `pip install -i ${joinUrlPath(entry.url, 'simple')} requests`),
          guideSnippet('pip-conf', 'pip.conf', '写入 pip.conf 后，后续 pip install 会默认通过当前代理访问 Simple API。', `[global]\nindex-url = ${joinUrlPath(entry.url, 'simple')}`)
        ]
      };
    case ProxyMode.Npm:
      return {
        entryLabel: entry.label,
        entryUrl: entry.url,
        snippets: [
          guideSnippet('npm', 'npm', '设置 registry 后，npm、pnpm、yarn 的包元数据和下载请求都会先经过当前代理。', `npm config set registry ${entry.url}`)
        ]
      };
    case ProxyMode.Oci:
      return {
        entryLabel: entry.label,
        entryUrl: entry.url,
        snippets: [
          guideSnippet('docker', 'Docker', '直接从当前镜像代理拉取示例镜像，适合验证客户端到代理的链路是否正常。', `docker pull ${joinUrlPath(stripScheme(entry.url), 'library/alpine:latest')}`)
        ]
      };
    default:
      return {
        entryLabel: entry.label,
        entryUrl: entry.url,
        snippets: [
          guideSnippet('curl', 'curl', '使用一个普通文件路径验证当前实例的入口是否可访问。', `curl ${joinUrlPath(entry.url, 'example.tar.gz')}`)
        ]
      };
  }
}

function guideSnippet(id: string, label: string, description: string, code: string): InstanceGuideSnippet {
  return { id, label, description, code };
}

function proxyEntry(instance: InstanceSummary): { label: string; url: string } {
  return {
    label: instance.entryLabel || (instance.entryKind === 'path' ? '路径入口' : '独立端口'),
    url: instance.entryUrl || instance.publicUrl || fallbackEntryURL(instance)
  };
}

function stripScheme(value: string): string {
  return value.replace(/^https?:\/\//, '');
}

function joinUrlPath(base: string, suffix: string): string {
  const cleanBase = base.replace(/\/+$/, '');
  const cleanSuffix = suffix.replace(/^\/+/, '');
  return `${cleanBase}/${cleanSuffix}`;
}

function fallbackEntryURL(instance: InstanceSummary): string {
  if (instance.path) {
    return instance.path;
  }
  return bindEntryURL(instance.bind ?? '');
}

function bindEntryURL(bind: string): string {
  if (bind.startsWith('http://') || bind.startsWith('https://')) return bind;
  const parsed = parseBind(bind);
  if (!parsed) return `http://${bind}`;
  const host = parsed.host.includes(':') && !parsed.host.startsWith('[') ? `[${parsed.host}]` : parsed.host;
  return `http://${host}:${parsed.port}`;
}

function parseBind(bind: string): { host: string; port: string } | undefined {
  const value = bind.trim();
  if (!value) return undefined;
  if (value.startsWith('[')) {
    const end = value.indexOf(']');
    if (end <= 0 || value[end + 1] !== ':') return undefined;
    return { host: value.slice(1, end), port: value.slice(end + 2) };
  }
  const index = value.lastIndexOf(':');
  if (index <= 0 || index === value.length - 1) return undefined;
  return { host: value.slice(0, index), port: value.slice(index + 1) };
}
