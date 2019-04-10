
Pod::Spec.new do |s|
  s.name         = "SQLite"
  s.version      = "0.1.0"
  s.summary      = "Lightweight wrapper around SQLite3's C API"
  s.swift_version = '4.2'

  s.description  = <<-DESC
    SQLite is a simple wrapper around SQLite3's C api
    DESC

  s.homepage     = "https://github.com/vdka/SQLite.swift"
  s.license      = { :type => "MIT", :file => "LICENSE.md" }
  s.author       = "Ethan Jackwitz"
  s.ios.deployment_target = "10.0"
  s.source       = { :git => "https://github.com/vdka/SQLite.swift.git", :tag => "#{s.version}" }
  s.source_files = "Sources", "Sources/SQLite/*.swift", "Sources/*.h"
  s.library = ["sqlite3"]
end

