FROM gcr.io/distroless/static:nonroot
USER nonroot:nonroot
ADD --chown=nonroot:nonroot windshift /app
ENTRYPOINT ["/app"]
