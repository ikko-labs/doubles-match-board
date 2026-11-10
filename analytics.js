// Vercel Web Analytics
import { inject } from '@vercel/analytics';

inject({
  beforeSend: (event) => {
    // Strip hash fragments from URLs to avoid exposing operation keys
    return {
      ...event,
      url: event.url.split('#')[0]
    };
  }
});
