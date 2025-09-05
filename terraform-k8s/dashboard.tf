resource "kubernetes_deployment" "dashboard" {
  metadata {
    name = "dashboard"
    namespace = "${var.namespace}"
    labels = {
      "k8s.service" = "dashboard"
    }
  }

  depends_on = [ kubernetes_service.cassandra ]

  spec {
    replicas = 1

    selector {
      match_labels = {
        "k8s.service" = "dashboard"
      }
    }

    template {
      metadata {
        labels = {
            "k8s.service" = "dashboard"
            "k8s.network/pipeline-network" = "true"
        }
      }

      spec {
        container {
          name = "dashboard"
          image = "nama1arpit/flask_dashboard:latest"
          image_pull_policy = "Always"

          port {
            container_port = 5000
          }

          # Environment variables
          env {
            name = "FLASK_ENV"
            value = "production"
          }

          resources {
            limits = {
              memory = "512Mi"
              cpu    = "500m"
            }
            requests = {
              memory = "256Mi"
              cpu    = "250m"
            }
          }
        }
      }
    }
  }
}

resource "kubernetes_service" "dashboard" {
  metadata {
    name = "dashboard"
    namespace = "${var.namespace}"
    labels = {
        "k8s.service" = "dashboard"
    }
  }

  depends_on = [ kubernetes_deployment.dashboard ]

  spec {
    selector = {
        "k8s.service" = "dashboard"
    }

    port {
      name        = "http"
      port        = 5000
      target_port = 5000
      node_port   = 30002
    }

    type = "NodePort"
  }
}