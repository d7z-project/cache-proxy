%{!?fixture_version:%global fixture_version 1.0.0}
%{!?fixture_state:%global fixture_state initial}

Name: e2e-rpm
Version: %{fixture_version}
Release: 1
Summary: cache-proxy E2E fixture
License: MIT
BuildArch: noarch

%description
cache-proxy end-to-end fixture

%install
mkdir -p %{buildroot}/usr/share/e2e-rpm
echo cache-proxy-e2e-%{fixture_state} > %{buildroot}/usr/share/e2e-rpm/payload.txt

%files
/usr/share/e2e-rpm/payload.txt
