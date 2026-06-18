import { createApp } from 'vue'
import { createPinia } from 'pinia'
import piniaPluginPersistedstate from 'pinia-plugin-persistedstate'
import router from './router'
import App from './App.vue'

import 'bootstrap/dist/css/bootstrap.min.css'
import 'bootstrap/dist/js/bootstrap.bundle.min.js'
import './assets/markdown.css'

import { library } from '@fortawesome/fontawesome-svg-core'
import { FontAwesomeIcon } from '@fortawesome/vue-fontawesome'
import {
  faBrain,
  faMagnifyingGlass,
  faTrash,
  faRotate,
  faRightFromBracket,
  faServer,
  faCircleCheck,
  faCircleXmark,
  faTag,
  faSpinner,
  faDatabase,
  faLayerGroup,
  faComments,
  faPlus,
  faLink,
  faLinkSlash,
  faCloud,
  faListCheck,
  faArrowRotateLeft,
  faTriangleExclamation,
  faDownload,
  faPen,
  faFire,
  faClockRotateLeft,
  faSliders,
  faUpload,
  faBoxArchive,
} from '@fortawesome/free-solid-svg-icons'

library.add(
  faBrain,
  faMagnifyingGlass,
  faTrash,
  faRotate,
  faRightFromBracket,
  faServer,
  faCircleCheck,
  faCircleXmark,
  faTag,
  faSpinner,
  faDatabase,
  faLayerGroup,
  faComments,
  faPlus,
  faLink,
  faLinkSlash,
  faCloud,
  faListCheck,
  faArrowRotateLeft,
  faTriangleExclamation,
  faDownload,
  faPen,
  faFire,
  faClockRotateLeft,
  faSliders,
  faUpload,
  faBoxArchive
)

const pinia = createPinia()
pinia.use(piniaPluginPersistedstate)

const app = createApp(App)
app.use(pinia)
app.use(router)
app.component('font-awesome-icon', FontAwesomeIcon)
app.mount('#app')
