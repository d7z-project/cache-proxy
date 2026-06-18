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
  entryDescription: string;
  snippets: InstanceGuideSnippet[];
}

export function buildInstanceGuide(instance: InstanceSummary): InstanceGuide {
  const entry = proxyEntry(instance);
  switch (instance.mode) {
    case ProxyMode.Go:
      return {
        entryLabel: entry.label,
        entryUrl: entry.url,
        entryDescription: 'Go 客户端通过 GOPROXY 访问模块元数据、zip 和 SumDB。',
        snippets: [
          guideSnippet('go', 'go', '临时指定 GOPROXY，适合先验证模块下载是否能正常经过当前代理。', `GOPROXY=${entry.url} go mod download`)
        ]
      };
    case ProxyMode.Maven:
      return {
        entryLabel: entry.label,
        entryUrl: entry.url,
        entryDescription: 'Maven 与 Gradle 直接把这个入口当作 Maven 仓库使用。',
        snippets: [
          guideSnippet('maven-xml', 'Maven', '将仓库片段加入 pom.xml 的 repositories 节点，用于常规 Maven 依赖下载。', `<repository>\n  <id>${instance.name}</id>\n  <url>${entry.url}</url>\n</repository>`),
          guideSnippet('gradle', 'Gradle', '将仓库片段加入 build.gradle 或 settings.gradle 的 repositories 块。', `repositories {\n  maven(url = "${entry.url}")\n}`)
        ]
      };
    case ProxyMode.Cargo:
      return {
        entryLabel: entry.label,
        entryUrl: entry.url,
        entryDescription: 'Cargo 使用 sparse index 入口，并通过同一入口回落到 crate 下载地址。',
        snippets: [
          guideSnippet('cargo-config', 'Cargo', '写入 .cargo/config.toml，使用 sparse registry 通过当前代理下载 crate。', `[registries.${instance.name}]\nprotocol = "sparse"\nindex = "${entry.url}"`)
        ]
      };
    case ProxyMode.PyPI:
      return {
        entryLabel: entry.label,
        entryUrl: joinUrlPath(entry.url, 'simple'),
        entryDescription: 'Python 客户端应访问 Simple API 入口，而不是实例根路径。',
        snippets: [
          guideSnippet('pip', 'pip', '单次安装时临时指定 index-url，适合先验证请求是否命中当前代理。', `pip install -i ${joinUrlPath(entry.url, 'simple')} requests`),
          guideSnippet('pip-conf', 'pip.conf', '写入 pip.conf 后，后续 pip install 会默认通过当前代理访问 Simple API。', `[global]\nindex-url = ${joinUrlPath(entry.url, 'simple')}`)
        ]
      };
    case ProxyMode.Npm:
      return {
        entryLabel: entry.label,
        entryUrl: entry.url,
        entryDescription: 'npm、pnpm、yarn 都把这个入口当作 registry 地址。',
        snippets: [
          guideSnippet('npm', 'npm', '设置 registry 后，npm、pnpm、yarn 的包元数据和下载请求都会先经过当前代理。', `npm config set registry ${entry.url}`)
        ]
      };
    case ProxyMode.Oci:
      return {
        entryLabel: entry.label,
        entryUrl: entry.url,
        entryDescription: '容器客户端直接把该独立入口当作 registry 主机地址。',
        snippets: [
          guideSnippet('docker', 'Docker', '直接从当前镜像代理拉取示例镜像，适合验证客户端到代理的链路是否正常。', `docker pull ${joinUrlPath(stripScheme(entry.url), 'library/alpine:latest')}`)
        ]
      };
    case ProxyMode.Apk:
      return {
        entryLabel: entry.label,
        entryUrl: entry.url,
        entryDescription: 'Alpine `apk` 通过仓库根路径访问 APKINDEX 和 `.apk` 文件。',
        snippets: [
          guideSnippet('apk-repo', 'repositories', '把实例入口写入 `/etc/apk/repositories`，客户端会直接读取 APKINDEX 与包文件。', `${entry.url}/main\n${entry.url}/community`)
        ]
      };
    case ProxyMode.Deb:
      return {
        entryLabel: entry.label,
        entryUrl: entry.url,
        entryDescription: 'APT 使用仓库根路径，随后自动访问 `dists/` 与 `pool/`。',
        snippets: [
          guideSnippet('deb-source', 'sources.list', '把仓库根路径写入 APT 源配置。', `deb ${entry.url} stable main`)
        ]
      };
    case ProxyMode.Rpm:
      return {
        entryLabel: entry.label,
        entryUrl: entry.url,
        entryDescription: 'YUM/DNF 使用仓库根路径，客户端会读取 `repodata/` 和 RPM 包。',
        snippets: [
          guideSnippet('rpm-repo', 'dnf repo', '将实例入口填入 `.repo` 文件中的 `baseurl`。', `[${instance.name}]\nname=${instance.name}\nbaseurl=${entry.url}\nenabled=1\ngpgcheck=0`)
        ]
      };
    case ProxyMode.Pacman:
      return {
        entryLabel: entry.label,
        entryUrl: entry.url,
        entryDescription: 'Pacman 使用仓库根路径，客户端会请求 `.db` 与 `.pkg.tar.*` 文件。',
        snippets: [
          guideSnippet('pacman-conf', 'pacman.conf', '把入口写入 `/etc/pacman.conf` 的 `Server`。', `[${instance.name}]\nServer = ${entry.url}/$repo/os/$arch`)
        ]
      };
    default:
      return {
        entryLabel: entry.label,
        entryUrl: entry.url,
        entryDescription: '普通文件代理直接以入口路径作为下载前缀。',
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
