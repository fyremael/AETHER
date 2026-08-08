import type { Metadata } from "next";
import { headers } from "next/headers";
import "./globals.css";

export async function generateMetadata(): Promise<Metadata> {
  const requestHeaders = await headers();
  const host =
    requestHeaders.get("x-forwarded-host") ??
    requestHeaders.get("host") ??
    "localhost:3000";
  const protocol =
    requestHeaders.get("x-forwarded-proto") ??
    (host.startsWith("localhost") ? "http" : "https");
  const baseUrl = new URL(`${protocol}://${host}`);
  const imageUrl = new URL("/og.png", baseUrl).toString();
  return {
    metadataBase: baseUrl,
    title: "AETHER Orbital — Frontier Engineering Fabric",
    description:
      "A temporal operations control room for AETHER's frontier engineering fabric: live topology, replay, evidence freshness, authority, and explainable promotion.",
    openGraph: {
      title: "AETHER Orbital",
      description: "Operational truth, in motion.",
      type: "website",
      images: [{ url: imageUrl, width: 1536, height: 1024, alt: "AETHER Orbital semantic fabric" }],
    },
    twitter: {
      card: "summary_large_image",
      title: "AETHER Orbital",
      description: "Operational truth, in motion.",
      images: [imageUrl],
    },
  };
}

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
