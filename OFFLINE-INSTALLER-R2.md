# Offline installer revision 2

This installer revision suppresses pip optional IPv6 and User-Agent platform/compiler probes during offline installation. It does not relax network or subprocess audit guards, does not change source/wheel product identity, and does not modify live authentication or dispatch. Ordinary and kernel-denied-network installation must both pass. Real Cloud execution still requires separate verification.
