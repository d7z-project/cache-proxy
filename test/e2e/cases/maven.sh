#!/usr/bin/env bash

e2e_prepare_maven() {
  e2e_build_image "$E2E_MAVEN_CLIENT_IMAGE" "$E2E_ROOT/test/e2e/clients/maven/Containerfile" "$E2E_ROOT/test/e2e/clients/maven"
}

e2e_run_maven() {
	printf '\n[maven] dependency resolution, warm artifact, metadata update and persisted offline reuse\n'
	e2e_reset_fixture
	e2e_assert_transparent_paths maven /maven /maven cache
  local script='
    mvn -B -ntp \
      -DremoteRepositories=e2e::default::"$1/maven" \
      org.apache.maven.plugins:maven-dependency-plugin:3.8.1:get \
      -Dartifact="com.example:e2e-maven:$2" -Dtransitive=false >/dev/null
    test -f "/root/.m2/repository/com/example/e2e-maven/$2/e2e-maven-$2.jar"
  '
  e2e_client maven cold "$E2E_MAVEN_CLIENT_IMAGE" "$script" "$E2E_PROXY_URL" 1.0.0
  local before
  before=$(e2e_fixture_count TRANSFER /maven/com/example/e2e-maven/1.0.0/e2e-maven-1.0.0.jar)
  ((before >= 1)) || e2e_fail 'Maven artifact did not reach the fixture'
  e2e_client maven warm "$E2E_MAVEN_CLIENT_IMAGE" "$script" "$E2E_PROXY_URL" 1.0.0
  e2e_assert_count_unchanged TRANSFER /maven/com/example/e2e-maven/1.0.0/e2e-maven-1.0.0.jar "$before" 'Maven artifact body was transferred during warm resolution'
  e2e_set_fixture_state updated
  e2e_wait_contains "$E2E_PROXY_URL/maven/com/example/e2e-maven/maven-metadata.xml" '<latest>2.0.0</latest>'
  e2e_client maven update "$E2E_MAVEN_CLIENT_IMAGE" "$script" "$E2E_PROXY_URL" 2.0.0
  e2e_offline_restart
  e2e_client maven offline "$E2E_MAVEN_CLIENT_IMAGE" "$script" "$E2E_PROXY_URL" 2.0.0
  e2e_restore_online
}
