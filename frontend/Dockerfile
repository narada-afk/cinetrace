FROM node:20-alpine

WORKDIR /app

COPY package.json package-lock.json ./
RUN npm ci

COPY . .

# Build the production bundle at image build time.
# NEXT_PUBLIC_* vars are baked into the JS bundle — must be passed as build args.
ARG NEXT_PUBLIC_API_URL=http://localhost:8000
ARG API_URL=http://localhost:8000
ARG NEXT_PUBLIC_GA_ID=""
ARG NEXT_PUBLIC_CLARITY_ID=""
ARG NEXT_PUBLIC_POSTHOG_KEY=""

ENV NEXT_PUBLIC_API_URL=$NEXT_PUBLIC_API_URL
ENV API_URL=$API_URL
ENV NEXT_PUBLIC_GA_ID=$NEXT_PUBLIC_GA_ID
ENV NEXT_PUBLIC_CLARITY_ID=$NEXT_PUBLIC_CLARITY_ID
ENV NEXT_PUBLIC_POSTHOG_KEY=$NEXT_PUBLIC_POSTHOG_KEY

RUN npm run build

EXPOSE 3000

# Run the production server (fast, pre-compiled)
CMD ["npm", "run", "start"]
