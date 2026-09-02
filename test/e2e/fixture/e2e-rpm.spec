Name: e2e-rpm
Version: 1.0.0
Release: 1
Summary: cache-proxy end-to-end fixture
License: MIT
BuildArch: noarch

%description
cache-proxy end-to-end fixture

%install
mkdir -p %{buildroot}/usr/share/e2e-rpm
echo cache-proxy-e2e > %{buildroot}/usr/share/e2e-rpm/payload.txt

%files
/usr/share/e2e-rpm/payload.txt
