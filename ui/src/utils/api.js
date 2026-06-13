import axios from 'axios'

// Plain JSON client for the one non-Connect endpoint: /auth/login. Everything
// else goes through the typed Connect client in connect.js.
const api = axios.create({ baseURL: window.location.origin })

export async function login(username, password) {
  const res = await api.post('/auth/login', { username, password })
  return res.data.token
}
