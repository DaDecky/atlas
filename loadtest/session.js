import http from 'k6/http';
import { Counter } from 'k6/metrics';

const authFailures = new Counter('auth_failures');
const authParams = { headers: { 'Content-Type': 'application/json' }, tags: { kind: 'auth' } };

// clientSession logs one client in and refreshes its token. Module scope is per VU, so each VU keeps its own session.
export function clientSession(authURL, clientID, clientSecret) {
  let session = null;

  return function accessToken() {
    if (session && Date.now() < session.refreshAt) {
      return session.access;
    }

    const res = session
      ? http.post(`${authURL}/auth/refresh`, JSON.stringify({ refresh_token: session.refresh }), authParams)
      : http.post(`${authURL}/auth/login`, JSON.stringify({ client_id: clientID, client_secret: clientSecret }), authParams);

    if (res.status !== 200) {
      authFailures.add(1);

      session = null;

      return null;
    }

    const body = res.json();

    // Refresh at 75% of the TTL so a token never expires mid-request.
    session = { access: body.access_token, refresh: body.refresh_token, refreshAt: Date.now() + body.expires_in * 750 };

    return session.access;
  };
}
